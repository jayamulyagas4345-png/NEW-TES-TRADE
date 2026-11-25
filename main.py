"""
╔══════════════════════════════════════════════════════════════════════════╗
║  BOT AUTO TRADE — SIGNAL ENGINE (SIGNALONLY) + EXECUTION SYSTEM (v67)     ║
║  BUILD: SIMPLE LIVE VERSION — v2 (+ pre-flight check, fail alert)         ║
║                                                                            ║
║  Sinyal/analysis engine: 1:1 sama dengan BOTSINYAL_ONLY_BASE.py           ║
║  (winrate proven, tidak diubah sama sekali).                              ║
║                                                                            ║
║  Sistem eksekusi live di-copy dari main__67_.py, tapi disederhanakan.     ║
║  Fitur yang DIPERTAHANKAN (sesuai permintaan):                            ║
║    • Auto trading real ke Binance Futures (order entry, TP, SL)          ║
║    • Mode API DEMO (testnet) / LIVE (mainnet) — /changeliveordemo         ║
║    • Cek & kirim IP publik server sebelum mulai (whitelist Binance)       ║
║    • /pnl        — unrealized + cumulative PnL                           ║
║    • /status     — status bot & posisi aktif                             ║
║    • /closeallposition — tutup semua posisi sekarang                     ║
║    • /maxopentrade <n> — set max posisi bersamaan                        ║
║    • /pairs [all|N|BTC,ETH] — atur pair yang dipantau (bawaan: semua)    ║
║    • /maxtradesperday <n> — batas trade dibuka per hari (UTC, 0=off)      ║
║    • /setmarginratio <persen> — max loss % per trade jika SL kena         ║
║    • /resumeorpause, /start, /resetpnl, /help (kontrol dasar)             ║
║    • Auto cleanup orphan SL/TP order tiap 5 menit (tanpa command manual)    ║
║    • /cleanuporders — cleanup orphan order manual (cek cepat kalau mau)   ║
║                                                                            ║
║  TAMBAHAN v2 (saran yang diminta ditambahkan):                           ║
║    • verify_api_connectivity() — /start SEKARANG cek koneksi+API key ke   ║
║      Binance dulu (call /fapi/v2/account). Kalau gagal (-2015 IP belum   ║
║      whitelist, -2014 key format salah, -2008 key invalid, dll) → /start  ║
║      DITOLAK, bot tetap standby, user dikasih tau alasan persisnya.       ║
║    • Global fail-streak alert — kalau order gagal ≥5x BERTURUT-TURUT      ║
║      (lintas pair, bukan cuma 1 pair), bot kirim alert sekali ke Telegram ║
║      (kemungkinan API key/koneksi/saldo bermasalah). Reset otomatis       ║
║      begitu ada 1 order yang sukses lagi.                                 ║
║                                                                            ║
║  Fitur yang DIHAPUS dari main__67_.py (supaya simpel, tidak diminta):     ║
║    /changemargin, /changelev, /closeonlyprofit, /closeonlyloss,           ║
║    /setscoreupto, /togglebtcfilter, /togglehtfstochgate, /backtest,       ║
║    /scalpingonly, /intradayonly, /allmode, /longonly, /shortonly,         ║
║    /resetdirection, /settp1partial, /maxdailyloss, /maxdailywin,          ║
║    /resetdailylimit, /superscalpermode,                                  ║
║    /tradelog, /perfpair, /perftf, /tuning, /weeklysummary, /menu          ║
║  (fungsi-fungsi ini dihapus total dari file — bukan cuma di-nonaktifkan)  ║
║                                                                            ║
║  SNIPER ACCURACY FIXES (v3, audit presisi entry):                        ║
║    1. check_confirmation_candle() di-wire jadi GATE #3b (dulu dead code,  ║
║       tidak pernah dipanggil) — candle TERKINI wajib konfirmasi arah.    ║
║    2. REJECTION_LOOKBACK diturunkan 3 → 2 candle (rejection tidak boleh   ║
║       terlalu basi untuk entry presisi).                                 ║
║    3. GATE #4 baru — minimum terpisah untuk subtotal tier struktural     ║
║       inti (htf+bos+momentum+liquidity+entry_zone+ob_fvg_conf), supaya   ║
║       faktor soft (session/RSI/macro/EMA) tidak bisa menutupi struktur   ║
║       yang sebenarnya lemah.                                             ║
║    4. monitor_pending_limit_orders() sekarang cancel lebih awal kalau    ║
║       structure di entry TF berbalik arah (BOS lawan) sebelum limit      ║
║       order ke-fill — dulu cuma nunggu timeout/TP1-touch.                ║
║    5. GATE #0b baru — ADX juga dicek di entry TF, bukan cuma HTF (4H     ║
║       trending tapi titik entry-nya sendiri masih choppy).               ║
║    6. GATE #3c baru — volume floor keras (vol_rat >= 0.6), dulu volume   ║
║       cuma nambah skor tanpa batas bawah.                                ║
║    7. GATE divergence StochRSI diperluas ke SEMUA skenario sniper        ║
║       (BOS_PULLBACK, BREAKOUT_RETEST, CHOCH_FLIP) — dulu cuma            ║
║       BREAKOUT_RETEST.                                                   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import time
import hmac
import hashlib
import math
import threading
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from typing import Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import csv
import io


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 0 — PERSISTENCE (State disimpan ke disk, survive Railway restart)
# ═══════════════════════════════════════════════════════════════════════════
#
# Railway adalah stateless container — semua variabel in-memory hilang saat restart.
# Solusi: simpan state penting ke file JSON di direktori /data (Railway Volume)
# atau fallback ke direktori file ini jika Volume tidak di-mount.
#
# State yang di-persist:
#   • BOT_MODE, MARGIN_RATIO, FIXED_LEVERAGE, MAX_OPEN_TRADES, _user_set_max_trades
#   • bot_paused, MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM
#   • BTC_CORR_FILTER_ON, _ACTIVE_MODE_FILTER
#   • TP1_PARTIAL
#   • DAILY_LOSS_LIMIT_PCT, DAILY_WIN_LIMIT_PCT
#   • bot_state (wins/losses/streaks/PnL tracking)
#   • _daily_limit_state
#   • _sent_signal_hashes (cooldown dedup sinyal)
#   • _cooldown_map
#
# Cara setup Railway Volume:
#   1. Railway Dashboard → project → Add Volume
#   2. Mount Path: /data
#   3. Deploy ulang — bot otomatis pakai /data/bot_state.json

# ── Tentukan direktori penyimpanan ──────────────────────────────────────────
def _get_data_dir() -> str:
    """
    Prioritas:
    1. /data  (Railway Volume — persist lintas restart)
    2. Direktori file main.py (fallback lokal)
    """
    if os.path.isdir("/data"):
        return "/data"
    return os.path.dirname(os.path.abspath(__file__))

_STATE_FILE = os.path.join(_get_data_dir(), "bot_state.json")
_state_lock = threading.Lock()

def _serialize_state() -> dict:
    """Kumpulkan semua state global yang perlu di-persist ke dict JSON-safe."""
    # Konversi _sent_signal_hashes: datetime (insertion time) → ISO string
    sig_hashes_serial = {
        k: v.isoformat() for k, v in _sent_signal_hashes.items()
    } if "_sent_signal_hashes" in dir() else {}

    # Konversi _cooldown_map: datetime → ISO string
    cooldown_serial = {
        k: v.isoformat() for k, v in _cooldown_map.items()
    } if "_cooldown_map" in dir() else {}

    # bot_state: konversi datetime fields
    bs = dict(bot_state)
    if isinstance(bs.get("start_time"), datetime):
        bs["start_time"] = bs["start_time"].isoformat()

    # _daily_limit_state: sudah string/float/None — safe
    dls = dict(_daily_limit_state)

    # Konversi pending_limit_orders: datetime → ISO string agar JSON-safe
    # Field "mode" dan "signal" mungkin ada nested object non-JSON — handle dengan try
    pending_serial = {}
    for _sym, _pend in pending_limit_orders.items():
        try:
            _p = dict(_pend)
            if isinstance(_p.get("placed_at"), datetime):
                _p["placed_at"] = _p["placed_at"].isoformat()
            # Pastikan semua field JSON-serializable (skip field yang tidak bisa)
            json.dumps(_p)   # test serialize
            pending_serial[_sym] = _p
        except Exception:
            # Simpan hanya field minimal yang pasti JSON-safe
            try:
                pending_serial[_sym] = {
                    "order_id":  _pend.get("order_id"),
                    "direction": _pend.get("direction"),
                    "entry":     _pend.get("entry"),
                    "sl":        _pend.get("sl"),
                    "tp1":       _pend.get("tp1"),
                    "tp2":       _pend.get("tp2"),
                    "lot":       _pend.get("lot"),
                    "tp1_size":  _pend.get("tp1_size"),
                    "tp2_size":  _pend.get("tp2_size"),
                    "side":      _pend.get("side"),
                    "sl_side":   _pend.get("sl_side"),
                    "position_side": _pend.get("position_side", "BOTH"),
                    "tick_size": _pend.get("tick_size"),
                    "eff_prec":  _pend.get("eff_prec"),
                    "step_size": _pend.get("step_size"),
                    "mkt_step_size": _pend.get("mkt_step_size"),
                    "min_qty":   _pend.get("min_qty"),
                    "placed_at": _pend["placed_at"].isoformat() if isinstance(_pend.get("placed_at"), datetime) else _pend.get("placed_at"),
                }
            except Exception:
                pass  # skip jika benar-benar tidak bisa serialize

    # Konversi active_positions: datetime → ISO string (FIX: posisi aktif ikut di-persist
    # agar tidak hilang konteksnya — SL/TP struktur asli, tp1_hit, trailing_active — saat restart)
    active_pos_serial = {}
    for _sym, _pos in active_positions.items():
        try:
            _ap = dict(_pos)
            if isinstance(_ap.get("open_time"), datetime):
                _ap["open_time"] = _ap["open_time"].isoformat()
            json.dumps(_ap)   # test serialize
            active_pos_serial[_sym] = _ap
        except Exception:
            pass  # skip posisi yang field-nya tidak bisa di-serialize

    return {
        "BOT_MODE":                  BOT_MODE,
        "MAX_SL_LOSS_PCT":           MAX_SL_LOSS_PCT,
        "MARGIN_RATIO":              MAX_SL_LOSS_PCT,   # backward-compat
        "FIXED_LEVERAGE":            FIXED_LEVERAGE,
        "MAX_OPEN_TRADES":           MAX_OPEN_TRADES,
        "_user_set_max_trades":      _user_set_max_trades,
        "bot_paused":                bot_paused,
        "MIN_SCORE_CUSTOM":          MIN_SCORE_CUSTOM,
        "MIN_SCORE_RELAXED_CUSTOM":  MIN_SCORE_RELAXED_CUSTOM,
        "BTC_CORR_FILTER_ON":        BTC_CORR_FILTER_ON,
        "STOCH_POSITION_GATE_ENABLED": STOCH_POSITION_GATE_ENABLED,
        "_ACTIVE_MODE_FILTER":       _ACTIVE_MODE_FILTER,
        "_DIRECTION_FILTER":         _DIRECTION_FILTER,
        "_SUPER_SCALPER_MODE":       _SUPER_SCALPER_MODE,
        "_PRE_SUPERSCALPER_SNAPSHOT": _PRE_SUPERSCALPER_SNAPSHOT,
        "TP1_PARTIAL":               TP1_PARTIAL,
        "DAILY_LOSS_LIMIT_PCT":      DAILY_LOSS_LIMIT_PCT,
        "DAILY_WIN_LIMIT_PCT":       DAILY_WIN_LIMIT_PCT,
        "MAX_TRADES_PER_DAY":        MAX_TRADES_PER_DAY,
        "_PAIR_SELECTION":           (list(_PAIR_SELECTION) if _PAIR_SELECTION else None),
        "_trades_today_state":       dict(_trades_today_state),
        "bot_state":                 bs,
        "_daily_limit_state":        dls,
        "_signal_hashes":            sig_hashes_serial,
        "_cooldown_map":             cooldown_serial,
        "_pending_limit_orders":     pending_serial,
        "active_positions":          active_pos_serial,
        # FIX BUG #7: persist hourly report timer agar tidak spam notif saat restart
        "_last_hourly_report":       _last_hourly_report.isoformat() if _last_hourly_report else None,
        "_last_pos_direction":       dict(_last_pos_direction),
        "_saved_at":                 datetime.now(timezone.utc).isoformat(),
    }

def save_state():
    """
    Simpan state bot ke disk (thread-safe).
    Dipanggil setiap kali ada perubahan setting atau setelah trade.
    """
    try:
        with _state_lock:
            data = _serialize_state()
            tmp  = _STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp, _STATE_FILE)   # atomic replace — tidak corrupt jika crash
        print(f"💾 State tersimpan → {_STATE_FILE}")
    except Exception as e:
        print(f"⚠️  Gagal simpan state: {e}")

def load_state():
    """
    Load state dari disk saat startup.
    Jika file tidak ada atau rusak → pakai default (fresh start).
    Dipanggil SEKALI di awal main() sebelum telegram polling.
    """
    global BOT_MODE, MAX_SL_LOSS_PCT, MARGIN_RATIO, FIXED_LEVERAGE, MAX_OPEN_TRADES
    global _user_set_max_trades, bot_paused, MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM
    global BTC_CORR_FILTER_ON, _ACTIVE_MODE_FILTER, _DIRECTION_FILTER, TP1_PARTIAL
    global STOCH_POSITION_GATE_ENABLED
    global _PAIR_SELECTION
    global DAILY_LOSS_LIMIT_PCT, DAILY_WIN_LIMIT_PCT
    global MAX_TRADES_PER_DAY, _trades_today_state
    global bot_state, _daily_limit_state, _sent_signal_hashes, _cooldown_map
    global _SUPER_SCALPER_MODE, _PRE_SUPERSCALPER_SNAPSHOT
    global _last_hourly_report   # FIX BUG #7
    global active_positions      # FIX: restore posisi aktif agar tidak jadi "pre_existing" saat restart

    if not os.path.exists(_STATE_FILE):
        print(f"ℹ️  State file tidak ditemukan ({_STATE_FILE}) — mulai fresh.")
        return

    try:
        with open(_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        saved_at = data.get("_saved_at", "?")
        print(f"📂 Memuat state dari disk... (disimpan: {saved_at})")

        # ── Setting variables ──────────────────────────────────────────────
        BOT_MODE                 = data.get("BOT_MODE",                 BOT_MODE)
        # Load MAX_SL_LOSS_PCT — fallback ke MARGIN_RATIO lama untuk backward-compat
        _loaded_sl = data.get("MAX_SL_LOSS_PCT", data.get("MARGIN_RATIO", MAX_SL_LOSS_PCT))
        MAX_SL_LOSS_PCT          = _loaded_sl
        MARGIN_RATIO             = _loaded_sl   # alias
        FIXED_LEVERAGE           = data.get("FIXED_LEVERAGE",           FIXED_LEVERAGE)
        MAX_OPEN_TRADES          = data.get("MAX_OPEN_TRADES",          MAX_OPEN_TRADES)
        _user_set_max_trades     = data.get("_user_set_max_trades",     _user_set_max_trades)
        bot_paused               = data.get("bot_paused",               bot_paused)
        MIN_SCORE_CUSTOM         = data.get("MIN_SCORE_CUSTOM",         MIN_SCORE_CUSTOM)
        MIN_SCORE_RELAXED_CUSTOM = data.get("MIN_SCORE_RELAXED_CUSTOM", MIN_SCORE_RELAXED_CUSTOM)
        # 1:1 DENGAN BASE: filter korelasi BTC/BTCD SELALU aktif (base tidak punya
        # toggle). Kalau False, btcd_trend dipaksa "FLAT" -> macro_pts 0 -> skor beda.
        BTC_CORR_FILTER_ON       = True
        STOCH_POSITION_GATE_ENABLED = data.get("STOCH_POSITION_GATE_ENABLED", STOCH_POSITION_GATE_ENABLED)
        _ps = data.get("_PAIR_SELECTION")
        if isinstance(_ps, list):
            _ps = [p for p in _ps if p in BASE_PAIRS]
            _PAIR_SELECTION = _ps or None
        # 1:1 DENGAN BASE: filter mode/arah/super-scalper TIDAK di-restore dari
        # state lama — dikunci ke default ("ALL"/"ALL"/False) supaya tidak ada
        # filter tersembunyi yang mengubah sinyal.
        _ACTIVE_MODE_FILTER      = "ALL"
        _DIRECTION_FILTER        = "ALL"
        _SUPER_SCALPER_MODE      = False
        TP1_PARTIAL              = data.get("TP1_PARTIAL",              TP1_PARTIAL)
        # FIX GUARD (2026-09-15): TP1_PARTIAL yang tersimpan di state lama bisa
        # berisi 1.0 (peninggalan mode single-TP versi sebelumnya) — kalau
        # dibiarkan, tp1_size = lot penuh & tp2_size = 0 sehingga posisi baru
        # selalu close di 1 target saja (tp1==tp2), walau default kode sekarang
        # sudah 0.5 (2-TP: partial di TP1, sisa trailing ke TP2). Reset paksa
        # ke default kalau value yang di-load di luar rentang partial yang valid.
        if SINGLE_TP_MODE:
            TP1_PARTIAL = 1.0   # mode 1-TP: paksa full close di satu target
        elif not (0.0 < TP1_PARTIAL < 1.0):
            print(f"  ⚠️  TP1_PARTIAL dari state tidak valid ({TP1_PARTIAL}) — reset ke default 0.5 (mode 2-TP).")
            TP1_PARTIAL = 0.5
        DAILY_LOSS_LIMIT_PCT     = data.get("DAILY_LOSS_LIMIT_PCT",     DAILY_LOSS_LIMIT_PCT)
        DAILY_WIN_LIMIT_PCT      = data.get("DAILY_WIN_LIMIT_PCT",      DAILY_WIN_LIMIT_PCT)
        try:
            MAX_TRADES_PER_DAY = max(0, int(data.get("MAX_TRADES_PER_DAY", MAX_TRADES_PER_DAY)))
        except Exception:
            pass
        # hitungan hari ini: hanya dipakai kalau tanggalnya masih hari ini (UTC);
        # kalau sudah lewat hari, otomatis mulai dari 0.
        _tts = data.get("_trades_today_state") or {}
        if isinstance(_tts, dict) and _tts.get("date") == datetime.now(timezone.utc).strftime("%Y-%m-%d"):
            try:
                _trades_today_state["date"]  = _tts["date"]
                _trades_today_state["count"] = max(0, int(_tts.get("count", 0)))
            except Exception:
                pass

        # ── bot_state ──────────────────────────────────────────────────────
        if "bot_state" in data:
            loaded_bs = data["bot_state"]
            # Restore datetime field
            if isinstance(loaded_bs.get("start_time"), str):
                try:
                    loaded_bs["start_time"] = datetime.fromisoformat(loaded_bs["start_time"])
                except Exception:
                    loaded_bs["start_time"] = None
            bot_state.update(loaded_bs)

        # ── _daily_limit_state ─────────────────────────────────────────────
        if "_daily_limit_state" in data:
            _daily_limit_state.update(data["_daily_limit_state"])

        # ── _sent_signal_hashes (ISO string → datetime, insertion time) ─────
        if "_signal_hashes" in data:
            now = datetime.now(timezone.utc)
            for k, v_str in data["_signal_hashes"].items():
                try:
                    ts = datetime.fromisoformat(v_str)
                    if (now - ts).total_seconds() / 3600 < SIGNAL_HASH_TTL_HOURS:   # buang yang sudah expired
                        _sent_signal_hashes[k] = ts
                except Exception:
                    pass

        # ── _cooldown_map (ISO string → datetime) ─────────────────────────
        if "_cooldown_map" in data:
            now = datetime.now(timezone.utc)
            for k, v_str in data["_cooldown_map"].items():
                try:
                    until = datetime.fromisoformat(v_str)
                    if until > now:   # buang yang sudah expired
                        _cooldown_map[k] = until
                except Exception:
                    pass

        # ── active_positions — restore agar posisi yang sudah berjalan TIDAK dianggap
        # "pre_existing" (HP) lagi saat restart, dan SL/TP struktur asli + progress
        # trailing/tp1_hit tetap terjaga. sync_existing_positions() akan skip symbol
        # yang sudah ada di sini (lihat: `if symbol in active_positions: continue`).
        if "active_positions" in data:
            restored_pos = 0
            for sym, p in data["active_positions"].items():
                try:
                    _ot = p.get("open_time")
                    if isinstance(_ot, str):
                        try:
                            p["open_time"] = datetime.fromisoformat(_ot)
                        except Exception:
                            p["open_time"] = None
                    with _positions_lock:
                        active_positions[sym] = p
                    restored_pos += 1
                except Exception as _rape:
                    print(f"  ⚠️  Gagal restore active_position [{sym}]: {_rape}")
            if restored_pos:
                print(f"  📦 {restored_pos} active_positions di-restore dari disk (bukan HP lagi)")

        # ── _pending_limit_orders — restore agar tidak hilang saat restart ────
        # Ini mencegah posisi terbuka tanpa SL/TP karena restart terjadi
        # saat limit order sudah FILLED tapi _activate_position_from_limit belum dipanggil.
        if "_pending_limit_orders" in data:
            now = datetime.now(timezone.utc)
            from datetime import timedelta
            restored_pending = 0
            for sym, p in data["_pending_limit_orders"].items():
                try:
                    # Konversi placed_at ISO string → datetime (selalu timezone-aware UTC)
                    placed_at_raw = p.get("placed_at")
                    placed_at = None
                    if isinstance(placed_at_raw, str):
                        placed_at = datetime.fromisoformat(placed_at_raw)
                        if placed_at.tzinfo is None:
                            placed_at = placed_at.replace(tzinfo=timezone.utc)
                    # Buang pending yang sudah expired (> LIMIT_ORDER_TIMEOUT_MINUTES)
                    if placed_at and (now - placed_at).total_seconds() / 60 > LIMIT_ORDER_TIMEOUT_MINUTES:
                        print(f"  ⏰ [{sym}] Pending limit expired saat restart — cek status Binance dulu")
                        # ── CEK STATUS DULU — jangan langsung cancel, bisa saja sudah FILLED ──
                        # Bug sebelumnya: langsung DELETE tanpa cek → kalau sudah FILLED,
                        # posisi terbuka tapi SL/TP tidak pernah dipasang karena pending dibuang.
                        _expired_oid = p.get("order_id")
                        _restart_status = None
                        if _expired_oid:
                            try:
                                _ord_chk = api_get("/fapi/v1/order", {"symbol": sym, "orderId": _expired_oid}, signed=True)
                                _restart_status = _ord_chk.get("status", "")
                                print(f"  🔍 [{sym}] Status order saat restart: {_restart_status}")
                            except Exception as _sq:
                                print(f"  ⚠️  [{sym}] Gagal query status saat restart: {_sq}")

                        if _restart_status == "FILLED":
                            # Order sudah terisi — restore pending agar monitor loop aktifkan posisi
                            # (pasang SL/TP + masuk active_positions). Jangan cancel!
                            print(f"  ✅ [{sym}] Order FILLED saat restart — restore pending, SL/TP akan dipasang oleh monitor loop")
                            p["placed_at"] = placed_at or now
                            pending_limit_orders[sym] = p
                            restored_pending += 1
                        elif _restart_status in ("CANCELED", "EXPIRED", "REJECTED"):
                            # Sudah tidak aktif — bersihkan saja, tidak perlu cancel
                            print(f"  🗑  [{sym}] Order sudah {_restart_status} di Binance — bersihkan saja")
                            try:
                                cancel_open_orders(sym)
                            except Exception:
                                pass
                        else:
                            # NEW / PARTIALLY_FILLED / None (query gagal) → cancel sekarang
                            if _expired_oid:
                                try:
                                    api_delete("/fapi/v1/order", {"symbol": sym, "orderId": _expired_oid})
                                    print(f"  🗑  [{sym}] Limit order {_expired_oid} di-cancel saat restart (expired)")
                                except Exception as _ce:
                                    print(f"  ⚠️  [{sym}] Gagal cancel limit order saat restart: {_ce}")
                            try:
                                cancel_open_orders(sym)
                                print(f"  🗑  [{sym}] Semua open/conditional orders di-cancel saat restart")
                            except Exception as _ce2:
                                print(f"  ⚠️  [{sym}] Gagal cancel open orders saat restart: {_ce2}")
                        continue
                    p["placed_at"] = placed_at or now
                    pending_limit_orders[sym] = p
                    restored_pending += 1
                    print(f"  📥 Restore pending limit: {sym} {p.get('direction')} | Entry:{p.get('entry')} | OrderID:{p.get('order_id')}")
                except Exception as _rpe:
                    print(f"  ⚠️  Gagal restore pending [{sym}]: {_rpe}")
            if restored_pending:
                print(f"  📦 {restored_pending} pending limit orders di-restore dari disk")

        # restore arah posisi terakhir (dipakai set_cooldown_post_close)
        try:
            _lpd = data.get("_last_pos_direction") or {}
            if isinstance(_lpd, dict):
                _last_pos_direction.update({k: v for k, v in _lpd.items() if v in ("LONG", "SHORT")})
        except Exception:
            pass

        # FIX BUG #7: restore hourly report timer agar tidak spam notif saat restart
        _lhr_raw = data.get("_last_hourly_report")
        if _lhr_raw:
            try:
                _lhr_dt = datetime.fromisoformat(_lhr_raw)
                if _lhr_dt.tzinfo is None:
                    _lhr_dt = _lhr_dt.replace(tzinfo=timezone.utc)
                _last_hourly_report = _lhr_dt
            except Exception:
                pass  # format salah → biarkan None (kirim laporan saat pertama kali)

        print(
            f"✅ State berhasil dimuat:\n"
            f"   BOT_MODE={BOT_MODE} | bot_paused={bot_paused} | "
            f"MAX_SL={MAX_SL_LOSS_PCT*100:.1f}% | LEV_FIXED={FIXED_LEVERAGE} | "
            f"MAX_TRADES={MAX_OPEN_TRADES}\n"
            f"   Score={MIN_SCORE_CUSTOM} | BTC_FILTER={BTC_CORR_FILTER_ON} | "
            f"MODE_FILTER={_ACTIVE_MODE_FILTER}\n"
            f"   Signal hashes aktif: {len(_sent_signal_hashes)} | "
            f"Cooldown aktif: {len(_cooldown_map)}"
        )

    except Exception as e:
        print(f"⚠️  Gagal load state ({e}) — pakai default.")

def _auto_save_loop():
    """
    Background thread: auto-save state setiap 5 menit.
    Ini jaga-jaga agar state tersimpan walau tidak ada event command/trade.
    """
    while True:
        time.sleep(300)   # setiap 5 menit
        try:
            save_state()
        except Exception as e:
            print(f"⚠️  Auto-save error: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 1 — CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

# ── API Credentials — LIVE (Binance Mainnet) ────────────────────────────────
# WAJIB diisi lewat environment variable Railway: BINANCE_API_KEY /
# BINANCE_API_SECRET — TIDAK ADA fallback hardcode di sini (key lama yang
# pernah tertulis plaintext di source dianggap sudah kompromis; revoke key
# itu di Binance API Management dan generate baru untuk diisi ke Railway).
API_KEY_LIVE    = os.environ.get("BINANCE_API_KEY", "")
API_SECRET_LIVE = os.environ.get("BINANCE_API_SECRET", "")

# ── API Credentials — DEMO (Binance Testnet) ────────────────────────────────
# FIX v41: generate API key BARU dari https://demo.binance.com (situs testnet
# lama testnet.binancefuture.com sudah dimatikan Binance — lihat BASE_URL_DEMO
# di atas). API key testnet lama TIDAK kompatibel dengan sistem baru ini.
# Diisi lewat environment variable Railway (BINANCE_DEMO_API_KEY /
# BINANCE_DEMO_API_SECRET) — tidak ada fallback hardcode.
API_KEY_DEMO    = os.environ.get("BINANCE_DEMO_API_KEY", "")
API_SECRET_DEMO = os.environ.get("BINANCE_DEMO_API_SECRET", "")

def get_api_credentials() -> tuple[str, str]:
    """Kembalikan (API_KEY, API_SECRET) yang aktif sesuai BOT_MODE."""
    if BOT_MODE == "LIVE":
        return API_KEY_LIVE, API_SECRET_LIVE
    return API_KEY_DEMO, API_SECRET_DEMO

# WAJIB diisi lewat environment variable Railway: TELEGRAM_TOKEN /
# TELEGRAM_CHAT_ID — token lama yang pernah tertulis plaintext di source
# dianggap sudah kompromis; revoke via @BotFather ("/revoke") dan generate
# token baru untuk diisi ke Railway.
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Bot Mode: LIVE vs DEMO ───────────────────────────────────────────────────
# DEMO = testnet (tidak ada uang asli), LIVE = mainnet (uang asli!)
# Default: DEMO untuk keamanan. Ganti via /changeliveordemo di Telegram.
BOT_MODE = "DEMO"   # "DEMO" atau "LIVE"
BASE_URL_LIVE = "https://fapi.binance.com"

# FIX v41: Binance memigrasikan REST base URL testnet dari
# "testnet.binancefuture.com" (lama, SUDAH DIMATIKAN — terkonfirmasi
# ERR_CONNECTION_RESET bahkan lewat VPN luar Indonesia) ke
# "demo-fapi.binance.com" (baru, per dokumentasi resmi developers.binance.com
# dan situs demo.binance.com yang menggantikannya).
#
# ⚠️ WAJIB pakai API key BARU yang di-generate dari demo.binance.com —
# API key lama dari testnet.binancefuture.com tidak akan valid di sini.
# Domain tetap configurable via environment variable BINANCE_TESTNET_BASE_URL
# kalau suatu saat Binance migrasi lagi:
#   BINANCE_TESTNET_BASE_URL=https://demo-fapi.binance.com   (default sekarang)
BASE_URL_DEMO = os.environ.get("BINANCE_TESTNET_BASE_URL", "https://demo-fapi.binance.com").rstrip("/")

def get_base_url() -> str:
    """Kembalikan BASE_URL yang aktif sesuai BOT_MODE.
    Dipakai HANYA untuk endpoint yang terikat ke akun/eksekusi order
    (account, position, order, algoOrder) — supaya order beneran
    kekirim ke exchange yang sesuai (testnet saat DEMO, mainnet saat LIVE).
    """
    return BASE_URL_LIVE if BOT_MODE == "LIVE" else BASE_URL_DEMO

# FIX: candle OHLCV untuk ANALISA sinyal harus selalu dari mainnet, terlepas
# dari BOT_MODE. Sebelumnya fetch_ohlcv_realdata() ikut get_base_url(), jadi
# saat DEMO dia analisa dari data Binance TESTNET — order book & price action
# testnet beda dari market asli, bikin struktur HTF/D&S-zone/rejection-candle
# terbaca beda dari SIGNAL-ONLY (yang analisa dari mainnet lewat ccxt).
# Endpoint klines ini bersifat PUBLIC (tidak butuh API key), jadi aman selalu
# dipanggil ke mainnet meskipun order eksekusinya tetap ke testnet saat DEMO.
MARKET_DATA_BASE_URL = "https://fapi.binance.com"

# ── Trade Execution ─────────────────────────────────────────────────────────
AUTO_TRADING     = True
RISK_PER_TRADE   = 0.30          # 30% balance per trade — modal $50: 30%×15x=$225 notional ✅ penuhi minNotional Binance ($100)

# ── Max SL Loss Per Trade ────────────────────────────────────────────────────
# Setiap trade, lot disesuaikan agar kerugian maksimal (jika kena SL) tidak
# melebihi MAX_SL_LOSS_PCT × total_balance.
# Rumus: max_loss_usdt = MAX_SL_LOSS_PCT × balance
#        lot = max_loss_usdt / sl_distance_per_unit
# Contoh: balance $10.000, max SL 1%, SL jarak 2% dari entry →
#         max_loss=$100, lot=$100/($200×2%)=$100/$4=25 unit
# Lot boleh berapa saja selama potensi loss jika SL kena ≤ MAX_SL_LOSS_PCT × balance.
# Ganti persentase di bawah atau via /setmarginratio (misal 1 → maks rugi 1% dari balance)
MAX_SL_LOSS_PCT  = 0.005             # 0.5% dari total balance sebagai max loss per trade jika SL kena (v-limits 2026-11-17; /setmarginratio 0.5)
MARGIN_RATIO     = MAX_SL_LOSS_PCT   # alias backward-compat (dipakai di beberapa tempat lain)
MARGIN_SAFETY_FACTOR = 0.95          # sisakan 5% available balance sebagai buffer (fee, slippage, posisi lain)
MAX_OPEN_TRADES  = 1             # modal <$100 → max 1 posisi saja. Naik otomatis saat saldo bertambah
BALANCE_TO_USE   = 1.0           # gunakan 100% available balance
SINGLE_TP_MODE   = True   # True = 1 TP saja (full close), SL tetap 1
SINGLE_TP_R_MULT = 1.5    # jarak TP tunggal = 1.5 x jarak SL (ubah sesuai selera)
TP1_PARTIAL      = 1.0 if SINGLE_TP_MODE else 0.5   # (lama) DIAKTIFKAN LAGI (permintaan user): partial 50% di TP1,
                          # sisa 50% lot jalan trailing (SL geser ke BE begitu TP1
                          # kena) menuju TP2 — bukan lagi single-TP full-close.
TRAILING_TRIGGER = 1.2
# ── BE FEE BUFFER (final fix) ───────────────────────────────────────────────
# Backtest 58 trade: gross +18.5 USDT, setelah fee/slippage -26.6 USDT (drag ≈ 0.08R
# per trade). SL breakeven yang dipasang PERSIS di entry bikin sisa lot (50%) keluar
# dengan taker fee + slippage → "BE" sebenarnya rugi kecil. Buffer ini menggeser SL BE
# sedikit ke arah profit supaya BE ≈ impas setelah fee. 0 = perilaku lama (SL di entry).
# Default 0.08% ≈ taker 0.05% + slippage kecil. Naikkan kalau BE masih sering minus.
BE_FEE_BUFFER_PCT = 0.0008

# ── GATE JARAK SL MAKSIMUM SAAT ENTRY (final fix) ───────────────────────────
# Backtest 254 trade (2 jendela independen, Feb–Apr & Jun–Agu 2026):
#   SL ≤ 1.5% dari entry → 129 trade, +0.17R/trade (+217 USDT), TP1 tercapai 69%
#   SL >  1.5% dari entry → 125 trade, -0.27R/trade (-338 USDT), TP1 tercapai 48%
# Pola sama di kedua jendela & di semua potongan 1.0–2.0%. SL lebar = volatilitas
# tinggi (ATR ~2x) → entry reversal sering gagal. Sinyal dengan SL lebih jauh dari
# batas ini DI-SKIP (bukan di-clamp). Ditemukan post-hoc → WAJIB divalidasi di data
# out-of-sample (jalankan backtest jendela lain; bandingkan dengan --max-sl-pct 0).
# 0 = nonaktif (perilaku lama). Pecahan: 0.015 = 1.5%.
MAX_SL_ENTRY_PCT = 0.0   # NONAKTIF: tidak lolos validasi out-of-sample (jendela Agu–Okt: -0.27R). Isi 0.015 untuk aktifkan.


def be_target_price(entry: float, direction: str) -> float:
    """Target SL breakeven = entry digeser BE_FEE_BUFFER_PCT ke arah profit (cover fee)."""
    try:
        buf = float(BE_FEE_BUFFER_PCT or 0.0)
    except Exception:
        buf = 0.0
    if buf <= 0:
        return entry
    return entry * (1 + buf) if direction == "LONG" else entry * (1 - buf)

TP2_WAIT_MINUTES = 0   # Dinonaktifkan (23 Jul 2026) — trailing BE sudah lindungi downside setelah TP1,
                        # jadi timeout berbasis waktu ini gak perlu, biarkan price action murni yang tentukan TP2.
MAX_DRAWDOWN     = 0.2

# ── Dynamic Max Trades — scale otomatis sesuai saldo ────────────────────────
# Saldo naik → max posisi otomatis bertambah, proporsional dengan kapasitas margin.
# Tier ini bisa disesuaikan sesuai risk appetite kamu.
DYNAMIC_TRADE_TIERS = [
    # (min_balance_usd, max_open_trades)
    (0,     1),    # < $100        → 1 posisi (margin cukup untuk minNotional $100)
    (100,   2),    # $100–$200     → 2 posisi
    (200,   3),    # $200–$500     → 3 posisi
    (500,   4),    # $500–$1000    → 4 posisi
    (1000,  5),    # $1000–$3000   → 5 posisi
    (3000,  8),    # $3000+        → 8 posisi
]

# Flag: True jika user sudah set /maxopentrade secara manual → MAX_OPEN_TRADES jadi hard cap
_user_set_max_trades: bool = False

def get_dynamic_max_trades() -> int:
    """
    Hitung max posisi berdasarkan saldo saat ini (dynamic scaling).

    Jika user sudah set /maxopentrade secara manual (_user_set_max_trades=True):
      → langsung return MAX_OPEN_TRADES (skip dynamic tier sepenuhnya)
    Jika tidak (default):
      → pilih tier tertinggi yang saldo-nya terpenuhi dari DYNAMIC_TRADE_TIERS
    """
    # ── User sudah set manual → pakai nilai itu langsung, abaikan dynamic tier ──
    if _user_set_max_trades:
        return MAX_OPEN_TRADES

    # ── Dynamic: naik otomatis sesuai saldo ──────────────────────────────────
    try:
        bal = get_total_balance()
    except Exception:
        return MAX_OPEN_TRADES   # fallback ke nilai default
    # FIX: handle None (API error) — jangan crash saat IP ban
    if not bal:
        return MAX_OPEN_TRADES
    result = MAX_OPEN_TRADES
    for min_bal, max_trades in DYNAMIC_TRADE_TIERS:
        if bal >= min_bal:
            result = max_trades
    return result

# ── Leverage Dinamis per Tier Harga ─────────────────────────────────────────
# Prinsip: harga makin murah → volatilitas % makin tinggi → leverage lebih rendah
# Ini mencegah lot meledak untuk pair seperti PEPE/SHIB (harga nano)
LEVERAGE_TIERS = [
    # (min_price, max_price, leverage)
    # BTC dinaikkan ke 20x agar modal kecil bisa penuhi minNotional Binance ($100)
    # Contoh: balance $10, margin 6%=$0.6, 20x → notional=$12 (masih kurang, tapi lebih baik)
    # Untuk balance $50+: margin 6%=$3, 20x → notional=$60 (mendekati minNotional)
    (10_000,  float("inf"), 20),   # BTC range          → 20x (naik dari 10x)
    (1_000,   10_000,       20),   # ETH range          → 20x
    (100,     1_000,        20),   # BNB/SOL range      → 20x
    (10,      100,          20),   # LTC/BCH range      → 20x
    (1,       10,           20),   # LINK/ATOM range    → 20x
    (0.1,     1,            15),   # ADA/XRP range      → 15x
    (0.01,    0.1,          10),   # DOGE range         → 10x
    (0.001,   0.01,         10),   # low price range    → 10x
    (0,       0.001,        15),   # micro price (PEPE/SHIB) → 15x (naik dari 5x agar notional cukup penuhi minNotional)
]

# ── Leverage bracket cache (untuk get_max_leverage_for_notional) ─────────────
# Leverage MAKSIMUM yang diizinkan bracket resmi Binance untuk notional tsb
# (bisa 50x/75x/100x/125x tergantung pair & ukuran notional) — dipakai di DEMO
# maupun LIVE. Ini penting saat banyak posisi dibuka bersamaan dengan modal
# kecil per trade — leverage tinggi bikin margin per trade kecil, jadi cukup modal
# untuk banyak slot sekaligus tanpa nabrak -2019 (margin insufficient).
# TIDAK di-precap lagi di DEMO: kalau testnet diam-diam nurunin leverage
# (kejadian nyata: BTCUSDT hedge mode testnet mentok ~20x meski request 125x),
# itu sudah dideteksi & dikoreksi lewat verifikasi positionRisk + recalculate
# margin/lot setelah set_leverage() (lihat execute_trade) — jadi aman coba
# request bracket max dulu, biar exchange sendiri yang tentuin batas realnya.
LEVERAGE_HARD_CAP_DEMO = None    # testnet — no pre-cap, verifikasi post-set yang menangani
LEVERAGE_HARD_CAP_LIVE = None    # mainnet — None = pakai leverage maksimum dari bracket Binance apa adanya
MIN_LEVERAGE          = 1
FALLBACK_LEVERAGE     = 10     # fallback jika gagal ambil bracket dari Binance
_leverage_bracket_cache: dict = {}
_leverage_bracket_cache_time: dict = {}
LEVERAGE_BRACKET_CACHE_TTL = 3600  # 1 jam


def _get_leverage_brackets(symbol: str) -> list:
    """Ambil daftar leverage bracket Binance untuk symbol (cache 1 jam)."""
    now = time.time()
    if symbol in _leverage_bracket_cache and (now - _leverage_bracket_cache_time.get(symbol, 0)) < LEVERAGE_BRACKET_CACHE_TTL:
        return _leverage_bracket_cache[symbol]
    try:
        data = api_get("/fapi/v1/leverageBracket", {"symbol": symbol}, signed=True)
        brackets = []
        if isinstance(data, list) and data:
            brackets = data[0].get("brackets", []) if isinstance(data[0], dict) else []
        elif isinstance(data, dict):
            brackets = data.get("brackets", [])
        brackets = sorted(brackets, key=lambda b: float(b.get("notionalCap", b.get("cap", 0))))
        if brackets:
            _leverage_bracket_cache[symbol] = brackets
            _leverage_bracket_cache_time[symbol] = now
        return brackets
    except Exception as e:
        print(f"  ⚠️  get_leverage_brackets {symbol}: {e}")
        return _leverage_bracket_cache.get(symbol, [])


def get_max_leverage_for_notional(symbol: str, notional: float) -> int:
    """
    Leverage MAKSIMUM yang diizinkan Binance untuk notional posisi tertentu.
    Notional makin besar → leverage maks makin kecil (sesuai bracket Binance).
    Ini meminimalkan margin_required = notional / leverage → cegah -2019.
    """
    brackets = _get_leverage_brackets(symbol)
    if not brackets:
        lev = FALLBACK_LEVERAGE
    else:
        chosen = brackets[-1]  # default: bracket terbesar (leverage terkecil)
        for b in brackets:
            cap = float(b.get("notionalCap", b.get("cap", 0)))
            if notional <= cap:
                chosen = b
                break
        lev = int(chosen.get("initialLeverage", chosen.get("leverage", FALLBACK_LEVERAGE)))
    # FIX: cap dinamis sesuai BOT_MODE saat ini (bukan konstanta statis) —
    # bot bisa switch DEMO↔LIVE runtime via /switchmode, jadi cek live.
    _cap = LEVERAGE_HARD_CAP_DEMO if BOT_MODE != "LIVE" else LEVERAGE_HARD_CAP_LIVE
    if _cap:
        lev = min(lev, _cap)
    return max(MIN_LEVERAGE, lev)


def get_leverage_for_price(price: float, symbol: str = "") -> int:
    """Pilih leverage berdasarkan harga entry, atau per-symbol override jika ada."""
    # ── Per-symbol override (prioritas tertinggi) ─────────────────────────────
    if symbol and symbol in PER_SYMBOL_LEVERAGE:
        return PER_SYMBOL_LEVERAGE[symbol]
    for min_p, max_p, lev in LEVERAGE_TIERS:
        if min_p <= price < max_p:
            return lev
    return 5  # fallback konservatif

# ── Lot Size Safety ─────────────────────────────────────────────────────────
# Untuk modal kecil ($50-$100):
# MAX_RISK_PER_TRADE_USDT_RATIO dinaikkan agar notional bisa penuhi minNotional.
# MAX_NOTIONAL_RATIO dinaikkan agar budget lot tidak terlalu kecil.
MAX_RISK_PER_TRADE_USDT_RATIO = 0.80   # max 80% balance sebagai margin 1 trade — modal $50 butuh ini agar notional >= $100
# Naikan ratio notional agar bisa penuhi minNotional di semua pair
MAX_NOTIONAL_RATIO   = 0.80            # 80% dari kapasitas leverage (naik dari 25%)
# Minimum SL distance untuk cegah lot blow-up
MIN_SL_DISTANCE_PCT  = 0.008          # 0.8% minimum SL — naik dari 0.5%, hindari SL terlalu tipis kena noise
# FIX SL DIPAKSA CAP: kalau SL struktural (dari zona demand/supply) lebih
# rapat dari MIN_SL_DISTANCE_PCT, jangan buang strukturnya dan ganti flat
# persentase — lebarkan pakai kelipatan ATR ini sebagai gantinya. ATR
# representasi volatilitas asli pair saat itu, jadi SL dapat "ruang" dari
# noise/wick/liquidity-grab yang proporsional ke pair itu sendiri, bukan
# angka flat yang sama untuk semua pair (yang sering mendarat persis di
# area stop-hunt karena tidak menghormati struktur/volatilitas lokal).
#
# NILAI 0.3 (bukan 0.5): mode aktif SATU-SATUNYA sekarang adalah LOW_TF
# (entry 15m, cap SL max 1.2% — lihat SL_TP_CAPS["LOW_TF"]). User pakai bot
# ini untuk SCALPING CEPAT (masuk-keluar cepat, gamau ranging lama), jadi
# SL tidak boleh melebar berlebihan — makin lebar SL, makin jauh jarak TP
# (karena RR ratio tetap), makin lama posisi ditahan sampai kena TP/SL, dan
# makin sering sinyal ke-skip karena kebentur cap 1.2% di pair yang lagi
# volatile (ATR 4H tinggi). 0.3 dipilih supaya buffer baru terasa di ATR
# tinggi (≥3%) sambil tetap muat di cap 1.2% sampai ATR ~4% — 0.5 sering
# melebihi cap mulai ATR 2.5% sehingga sinyal jadi kena skip lebih sering.
SL_ATR_WIDEN_MULT    = 0.3            # 0.3× ATR 4H sebagai buffer pelebaran SL
# 1:1 main49: pelebaran SL otomatis di execute_trade() (SL < MIN_SL_DISTANCE_PCT lalu TP di-recalc)
# MENGUBAH entry/SL/TP dibanding sinyal asli, jadi angka trade beda dari bot signal-only.
# Default OFF → SL/TP persis sama dengan sinyal. Set True kalau mau pelebaran SL aktif lagi.
EXEC_SL_WIDEN_ENABLED = False

# ── Bot pause state ─────────────────────────────────────────────────────────
# Bot selalu mulai dalam kondisi PAUSED — kirim /start untuk mulai trading
bot_paused = True

# ── Daily Loss / Win Limit ───────────────────────────────────────────────────
# Set via /maxdailyloss dan /maxdailywin (dalam % dari total portfolio)
# 0.0 = tidak aktif (tidak ada limit)
# Contoh: 5.0 = bot auto-pause jika daily PnL mencapai -5% (loss) atau +5% (win)
DAILY_LOSS_LIMIT_PCT: float = 2.0   # % max daily loss dari total portfolio (0 = off)  [v-limits: 2%; bot_state.json bisa menimpa → cek /maxdailyloss]
DAILY_WIN_LIMIT_PCT:  float = 2.0   # % max daily win dari total portfolio (0 = off)  [v-limits: 2%; bot_state.json bisa menimpa → cek /maxdailywin]

# ── Limit MINGGUAN & BULANAN (v-limits 2026-11-17) ───────────────────────────
# Dihitung dari balance awal minggu (Senin 00:00 UTC) / awal bulan (tgl 1 00:00 UTC), sama seperti
# daily limit (balance-based; deposit/withdraw ikut terhitung). Kena limit → bot PAUSE sampai periode
# berikutnya lalu auto-start. 0 = nonaktif. Diatur lewat file ini (tidak lewat Telegram).
# CATATAN: pada start pertama, baseline minggu/bulan = balance saat bot pertama kali dicek.
WEEKLY_LOSS_LIMIT_PCT:  float = 4.0   # pause sampai Senin depan kalau PnL minggu ini <= -4%
MONTHLY_LOSS_LIMIT_PCT: float = 6.0   # pause sampai bulan depan kalau PnL bulan ini <= -6%
MONTHLY_WIN_LIMIT_PCT:  float = 8.0   # pause sampai bulan depan kalau PnL bulan ini >= +8% (kunci profit)

# State untuk tracking daily limit
# "date"          : tanggal UTC terakhir cek (format "YYYY-MM-DD")
# "balance_open"  : balance awal hari (UTC 00:00) untuk kalkulasi daily PnL
# "paused_by"     : None | "DAILY_LOSS" | "DAILY_WIN" — penyebab auto-pause limit
# "auto_started"  : apakah bot sudah auto-start hari ini setelah ganti hari
_daily_limit_state: dict = {
    "date":         None,
    "balance_open": 0.0,
    "paused_by":    None,
    "auto_started": False,
}

# ── Max Trades Per Hari ──────────────────────────────────────────────────────
# Batas jumlah trade yang boleh DIBUKA per hari UTC (reset 00:00 UTC).
# Set via /maxtradesperday <n>. 0 = tidak dibatasi.
# Yang dihitung: order yang benar-benar TERISI (jadi posisi aktif). Limit order
# yang di-cancel / expired / ditolak saldo TIDAK memakai jatah.
# Cek saat kirim order: terisi_hari_ini + pending_limit_orders >= batas -> skip,
# supaya limit order yang masih menunggu fill tidak bisa menembus batas.
MAX_TRADES_PER_DAY: int = 3   # v-limits: anti over-trade (bot_state.json bisa menimpa → cek /maxtradesperday)
_trades_today_state: dict = {"date": None, "count": 0}
_trades_today_lock = threading.Lock()

# ── Fixed Margin & Leverage Override ────────────────────────────────────────
# FIXED_LEVERAGE dipertahankan sebagai internal safety switch (dipakai di
# calculate_lot), tapi command /setfixedlev & /resetmm sudah dihapus — leverage
# sekarang selalu dinamis: auto-tier by price + bracket maksimum Binance (LIVE)
# atau dipentok 20x (DEMO/testnet). Biarkan tetap 0 (off).
FIXED_LEVERAGE: int      = 0     # 0 = off (pakai leverage tier otomatis) — jangan diubah manual

# ── Per-Symbol Leverage Override ─────────────────────────────────────────────
# Leverage khusus per symbol — override FIXED_LEVERAGE dan auto-tier.
# Contoh: XAUUSDT pakai 100x sementara pair lain pakai 10x atau auto-tier.
# Kosongkan dict ini untuk tidak ada override per-symbol.
PER_SYMBOL_LEVERAGE: dict = {
    "XAUUSDT":      100,  # Gold — leverage 100x (independen dari global setting)
    "BTCUSDT":       20,  # BTC harga ~$100k — 20x agar notional cukup penuhi minNotional
    "1000PEPEUSDT":  20,  # PEPE harga nano — 20x agar notional cukup
    "1000SHIBUSDT":  20,  # SHIB harga nano — 20x agar notional cukup
}

# ── BTC Filter Exemption (Pair Bebas dari BTC Correlation Gate) ───────────────
# Pair dalam set ini TIDAK kena BTC Multi-TF Direction Gate maupun BTC Corr Filter.
# Analisa mereka sepenuhnya independen dari arah BTC.
# Default: BTCUSDT (BTC tidak filter dirinya sendiri).
# Tambahan: XAUUSDT (Gold tidak berkorelasi dengan BTC — punya driver sendiri).
BTC_EXEMPT_PAIRS: set = {
    "BTCUSDT", "BTC/USDT",   # BTC tidak filter dirinya sendiri
    "XAUUSDT", "XAU/USDT",   # Gold — independen dari BTC, driver: DXY, suku bunga, geopolitik
}

# ── Hedge Mode detection ─────────────────────────────────────────────────────
_hedge_mode: bool | None = None   # None = belum dideteksi

def is_hedge_mode() -> bool:
    """Cek apakah akun pakai Hedge Mode (dualSidePosition=true). Cache hasilnya."""
    global _hedge_mode
    if _hedge_mode is not None:
        return _hedge_mode
    try:
        data = api_get("/fapi/v1/positionSide/dual", signed=True)
        _hedge_mode = bool(data.get("dualSidePosition", False))
        print(f"  ℹ️  Position mode: {'HEDGE' if _hedge_mode else 'ONE-WAY'}")
    except Exception as e:
        print(f"  ⚠️  Gagal deteksi position mode: {e} — asumsi ONE-WAY")
        _hedge_mode = False
    return _hedge_mode

# ── Scan Modes ──────────────────────────────────────────────────────────────
# LOW_TF (tier RELAXED) DIMATIKAN per 2026-09-01: trade log 21 sinyal (30 Aug -
# 1 Sep) menunjukkan winrate 23.8%, 16/21 full-SL tanpa TP1 partial sekalipun —
# konsisten dengan catatan lama bahwa LOW_TF winrate-nya kurang baik.
# LOW_TF entry lama dikomentari, bukan dihapus, tinggal un-comment kalau mau
# balik. INTRADAY / SCALPING / LTF_30M / BOS_FVG_4H masih nonaktif seperti
# sebelumnya (dihapus total dari _ALL_MODES di versi ini).
#
# DIBALIK per permintaan user: bias arah SEKARANG ditentukan di TF 4 JAM
# (htf_tf), setup/entry dicari di TF 1 JAM (entry_tf) — bukan lagi 1h/1h.
# Nama mode diganti HTF_1H → HTF_4H supaya cerminkan konfigurasi baru.
# Semua logika hilir (calculate_rr, compute_score, evaluate_htf_ranging, dst)
# generic terhadap TF, jadi tidak perlu diubah — cukup config mode ini yang
# menentukan df_htf datang dari TF mana dan df_entry dari TF mana.
_ALL_MODES = [
    # 1:1 PARITY FIX (2026-09-18): LOW_TF diaktifkan kembali, HTF_4H
    # dimatikan — sesuai permintaan user, autotrade WAJIB pakai mode yang
    # sama persis dengan signal-only base (signal.py), yaitu LOW_TF
    # (HTF=1h → Entry=15m, tier RELAXED). HTF_4H (4h/1h, tier FULL) tidak
    # ada padanannya di signal.py sama sekali, jadi tidak bisa 1:1 selama
    # aktif. Baris HTF_4H dikomentari, bukan dihapus, kalau nanti perlu
    # dipakai lagi.
    {"label": "LOW_TF",   "htf_tf": "1h",  "entry_tf": "30m", "ref_tf": None,  "tier": "RELAXED"},
    # {"label": "HTF_4H", "htf_tf": "4h", "entry_tf": "1h", "ref_tf": None, "tier": "FULL"},
]

# GOLD_MODES dihapus — tidak ada lagi mode khusus XAUUSDT. Mode LOW_TF
# (SMC) sekarang berlaku sama untuk semua pair termasuk XAUUSDT.

# ── Active Modes — diubah via /scalpingonly / /intradayonly ──────────────────
# "ALL"      = semua mode aktif (default)
# "SCALPING" = hanya LOW_TF + LTF_30M  (entry 15m/30m — scalping sejati)
# "INTRADAY" = INTRADAY + SCALPING     (entry 1h/4h — swing-intraday)
_ACTIVE_MODE_FILTER: str = "ALL"   # diubah via command

# ── Direction Filter — diubah via /longonly / /shortonly / /resetdirection ──
# "ALL"   = semua arah (default)
# "LONG"  = hanya sinyal BULLISH
# "SHORT" = hanya sinyal BEARISH
_DIRECTION_FILTER: str = "ALL"   # diubah via /longonly, /shortonly, /resetdirection

# ── Super Scalper Mode — diaktifkan via /superscalpermode ───────────────────
# Ketika ON:
#   • Hanya pair HIGH-LIQUIDITY (Tier 1 + beberapa Tier 2 dengan volume terbesar)
#   • Hanya mode LOW_TF (15m) + LTF_30M (30m) entry
#   • Score threshold diturunkan ke 35 agar sinyal lebih banyak
#   • Cooldown setelah SL dipersingkat ke 1 jam
#   • BTC correlation filter di-OFF agar tidak block sinyal momentum
#   • SCAN_INTERVAL dikurangi ke 20 detik (lebih cepat respons)
#   • TP1 fixed 0.8%, TP2 1.5% (scalp profit kecil tapi sering)
#   • RR minimum diturunkan ke 1.2 (scalper lebih sering keluar cepat)
_SUPER_SCALPER_MODE: bool = False

# Pair list khusus super scalper — HANYA coin dengan:
#   • Open Interest Binance Futures > $200M
#   • 24h Volume Futures > $500M rata-rata
#   • Spread tight bahkan di jam sepi (Asia session)
#   • Cukup volatil di 15m/30m untuk menghasilkan sinyal bersih
#
# Sumber referensi: Binance Futures top volume + OI ranking (Apr 2025)
# DIBUANG dari versi lama: HYPE (OI kecil), RUNE (spread lebar),
#   NEAR/OP/ARB/APT (volume drop di Asia session), TON (kurang liquid futures)
_SUPER_SCALPER_PAIRS = [
    # ── Tier S: Absolute Top — volume & OI terbesar, spread paling tight ────
    "BTCUSDT",   "ETHUSDT",   "SOLUSDT",   "XRPUSDT",   "BNBUSDT",
    # ── Tier A: High OI + Volume — konsisten 24 jam ──────────────────────────
    "DOGEUSDT",  "ADAUSDT",   "AVAXUSDT",  "LINKUSDT",   "LTCUSDT",
    "DOTUSDT",   "UNIUSDT",   "AAVEUSDT",  "SUIUSDT",    "1000PEPEUSDT",
    # ── Tier B: Volume cukup, volatilitas baik untuk 15m/30m ─────────────────
    "TRXUSDT",   "XLMUSDT",   "INJUSDT",   "ENAUSDT",    "1000SHIBUSDT",
]

# Snapshot settings sebelum super scalper aktif (untuk restore)
_PRE_SUPERSCALPER_SNAPSHOT: dict = {}

def get_active_modes() -> list:
    """1:1 DENGAN BASE: selalu _ALL_MODES (LOW_TF 1h->15m RELAXED).
    Filter mode/SUPER_SCALPER lama DIKUNCI — sebelumnya bisa aktif diam-diam
    lewat bot_state.json lama dan mengubah mode yang discan."""
    return list(_ALL_MODES)


# 1:1 DENGAN BASE (signal-only main46): 47 pair yang sama persis.
# 39 pair ekstra di PAIR_LIST lama (WIF, TAO, SEI, ENA, dst) TIDAK discan
# supaya himpunan sinyal identik dengan base.
BASE_PAIRS = [
    "BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT", "POLUSDT",
    "TRXUSDT", "AVAXUSDT", "LINKUSDT", "1000SHIBUSDT", "TONUSDT", "SUIUSDT", "DOTUSDT", "LTCUSDT",
    "BCHUSDT", "NEARUSDT", "APTUSDT", "UNIUSDT", "ICPUSDT", "1000PEPEUSDT", "ETCUSDT", "STXUSDT",
    "FILUSDT", "OPUSDT", "INJUSDT", "IMXUSDT", "ARBUSDT", "ATOMUSDT", "RENDERUSDT",
    "GRTUSDT", "SANDUSDT", "MANAUSDT", "AAVEUSDT", "THETAUSDT", "XLMUSDT", "ALGOUSDT", "AXSUSDT",
    "EGLDUSDT", "HBARUSDT", "QNTUSDT", "FLOWUSDT", "CHZUSDT", "GALAUSDT", "ZILUSDT",
    "HYPEUSDT",
]

# Pair yang dipantau bot, diatur lewat command /pairs (tersimpan di bot_state.json).
# None = semua BASE_PAIRS | list = hanya pair itu (selalu bagian dari BASE_PAIRS).
_PAIR_SELECTION = None

def get_active_pairs() -> list:
    """Pair aktif = BASE_PAIRS (1:1 dengan base signal-only), atau pilihan /pairs kalau diatur.
    Difilter ke symbol yang valid di exchange aktif (testnet punya daftar
    lebih sempit) — filter ini hanya membuang pair yang MEMANG tidak bisa
    di-trade di exchange itu, tidak mengubah logika sinyal."""
    base = list(_PAIR_SELECTION) if _PAIR_SELECTION else list(BASE_PAIRS)
    if not _exchange_info_cache:
        return base
    filtered = [p for p in base if p in _exchange_info_cache]
    return filtered if filtered else base

MODES = _ALL_MODES   # alias — dipakai di seluruh kode (tidak perlu ubah referensi lain)

# ── SL Distance Filter ───────────────────────────────────────────────────────
# CATATAN: ini SKIP-ONLY (safety net), BUKAN clamp — tidak pernah memepetkan/
# mengecilkan SL yang sudah dihitung struktural oleh calculate_rr(). Satu-satunya
# titik pakai (baris ~8332) membandingkan dengan toleransi ×3 dan HANYA men-skip
# entry kalau kelewat jauh (mis. >9% untuk cap 3%) — bukan mengubah nilai SL itu
# sendiri. SL tetap murni dari OB/FVG/swing terdekat + buffer ATR, seberapapun
# lebar hasilnya, selama masih di bawah ambang skip ini.
# Set ke 0 untuk non-aktif (tidak ada filter SL distance).
MAX_SL_DISTANCE_PCT: float = 0.03   # dipakai sbg fallback default kalau mode tidak ada di SL_TP_CAPS

# ── HARD SAFETY NET: SL width absolut, berlaku utk SEMUA mode ───────────────
# CATATAN: konstanta ini TIDAK dipakai — calculate_rr() sejak porting dari
# main20.py tidak lagi memanggil _apply_sl_width_gate() atau enforce cap apa
# pun di dalamnya. SL yang keluar dari calculate_rr() murni struktural (OB/
# FVG/swing terdekat + buffer ATR), tidak pernah di-cap/dipepetkan di sini.
# Dibiarkan ada untuk referensi/kompatibilitas command lain saja.
SL_WIDTH_HARD_MAX_PCT: float = 0.07   # 7% — TIDAK di-enforce, lihat catatan di atas

# ── Per-Mode SL & TP Cap ─────────────────────────────────────────────────────
# CATATAN PENTING: dict ini SKIP-ONLY (safety net terakhir di execute_trade,
# baris ~8331-8336), BUKAN clamp yang membentuk/memepetkan SL saat sinyal
# dibuat. calculate_rr() TIDAK membaca dict ini sama sekali — SL yang
# dihasilkan selalu murni struktural (zone_low/zone_high dari OB/FVG + buffer
# ATR proporsional), berapapun lebarnya. Nilai di sini hanya jadi ambang
# terakhir: kalau SL_final > cap×3, entry di-skip demi keamanan (mis. kalau
# ada bug rounding/tick yang bikin SL absurd) — SL itu sendiri tidak pernah
# diubah/dipepetkan supaya lolos cap ini.
SL_TP_CAPS: dict = {
    # label      : (max_sl_pct, max_tp_pct) — ambang skip (×3), bukan target/clamp
    "LOW_TF"    : (0.012, 0.030),
    # HTF_4H (dibalik dari HTF_1H): HTF bias sekarang dari 4h, entry TF tetap
    # 1h — SL tetap dihitung dari swing/OB/FVG di candle 1h (bukan 15m),
    # supaya SL nempel di bawah swing struktural yang sebenarnya, bukan "di
    # dalam" body candle kecil yang gampang ke-wick. Cap SL/TP tidak berubah
    # dari HTF_1H karena entry_tf-nya sama-sama 1h (yang beda cuma sumber
    # bias arahnya, dari 4h bukan 1h) — ambang skip 7%/17.5% tetap relevan.
    "HTF_4H"    : (0.07, 0.175),
    # LTF_30M: cap SL dilebarkan 1.5% → 2.2% (SNIPER FIX liquidity-sweep
    # safety) supaya buffer yang baru (0.6 ATR + wick 0.9%) tidak langsung
    # kepotong cap lama. TP ikut dilebarkan 3.5% → 5.0% agar RR tidak anjlok
    # akibat SL yang lebih lebar.
    "LTF_30M"   : (0.022, 0.050),   # entry 30m → SL max 2.2%, TP max 5.0%
    "SCALPING"  : (0.040, 0.080),   # entry 1h  → SL max 4%, TP max 8%
    "INTRADAY"  : (0.050, 0.120),   # entry 1h  → SL max 5%, TP max 12%
    # GOLD_SCALP: entry 5m — Gold volatil (ATR 5m bisa $5-15), SL lebih longgar dari altcoin.
    # SL 0.8% di harga $3300 = $26 — cukup beri napas noise 5m Gold.
    # TP 2.0% = $66 — realistis dalam 3-8 candle 5m di sesi London/NY.
    "GOLD_SCALP": (0.008, 0.020),   # entry 5m  → SL max 0.8%, TP max 2.0%
}

# ── Scoring khusus XAUUSDT (Gold) ────────────────────────────────────────────
# Logika pasar Gold berbeda dari altcoin:
#   • Respons kuat terhadap S/R psikologis (level round number $3000, $3100, dll)
#   • Liquidity sweep sangat reliable — institusi sering sweep lows/highs sebelum balik
#   • BOS di 5m penting: konfirmasi struktur micro sebelum entry
#   • Volume Gold futures tidak se-ekstrem crypto — threshold lebih rendah
#   • Session sangat kritis: London-NY overlap adalah waktu emas Gold bergerak
#
# Bobot lebih tinggi dari default untuk:
#   • Liquidity sweep (Gold sering false-break → sweep sangat meaningful)
#   • BOS konfirmasi (struktur 5m harus clear)
#   • Session (penalty besar kalau trading di luar London/NY)
#
# Bobot lebih rendah dari default untuk:
#   • Momentum consecutive candle (Gold sering choppy di 5m, susah N candle sama arah)
#   • Macro (BTC macro tidak relevan untuk Gold)
GOLD_SCORE_WEIGHTS: dict = {
    # Tier 1 HTF bias (30m) — penting tapi bukan satu-satunya
    "htf_aligned":        18,   # default: 15 → naikkan sedikit, 30m HTF harus sejalan
    "htf_penalty":       -12,   # default: -10 → lebih ketat jika melawan 30m bias

    # Tier 2 BOS — kritis untuk Gold 5m
    "bos_strong":         28,   # default: 25 → naikkan, BOS 5m Gold lebih signifikan
    "bos_weak":           14,   # default: 12
    "bos_penalty":       -12,   # default: -10 → lebih keras jika tidak ada BOS

    # Tier 3 Momentum — diturunkan, Gold 5m sering choppy
    "momentum_aligned":   10,   # default: 15 → turunkan, jangan terlalu bergantung N candle
    "momentum_none":       0,
    "momentum_anti":      -3,   # default: -5 → lebih permisif

    # Tier 4 Liquidity sweep — paling berharga untuk Gold
    "liq_strong":         20,   # default: 15 → dinaikkan, sweep di Gold sangat reliable
    "liq_weak":           12,   # default: 8
    "liq_none":           -3,   # default: -5 → sedikit lebih permisif

    # Tier 5 Entry zone — tetap penting
    "zone_double":        18,   # default: 15 → confluence lebih penting di 5m
    "zone_single":        12,   # default: 10
    "zone_none":           0,

    # Supporting
    "displacement":        8,   # default: 10 → sedikit dikurangi (Gold impulse bisa besar tapi sering noise)
    "session_london_ny":  10,   # default: 5 → session jauh lebih kritis untuk Gold
    "session_overlap":    15,   # bonus ekstra saat London-NY overlap (12:00-15:00 UTC)
    "session_asia":       -8,   # Asia session: penalti, Gold sepi & choppy
    "session_offhours":  -15,   # Off-hours: penalti besar, jangan trade Gold di luar jam utama

    # Score gate minimum khusus Gold
    # ALIGNED WITH BASE: dikembalikan ke 50 (catatan: key ini tidak pernah
    # dibaca di manapun untuk gating skor Gold — gate aktualnya tetap pakai
    # MIN_SCORE/MIN_SCORE_CUSTOM global, sama seperti mode lain)
    "min_score":          50,
}

SCAN_INTERVAL = 60          # ALIGNED WITH BASE (signal-only) — disamakan 1:1.
# FIX KRITIS SINGLE TP/structure reversal: manage_trailing() sekarang jalan di thread
# terpisah (lihat _trailing_monitor_loop), polling tiap TRAILING_POLL_INTERVAL
# detik — TIDAK lagi menunggu scan sinyal (SCAN_INTERVAL) selesai dulu.
TRAILING_POLL_INTERVAL = 8  # detik — cek SL/TP/structure reversal tiap 8 detik
# Sebelumnya 120 detik (scan tiap 2 menit) vs signal-only 60 detik (tiap 1
# menit). Ini bikin automatrade scan separuh sesering signal-only — window
# setup yang cuma valid sesaat (mis. breakout candle 15m yang baru kebentuk)
# bisa sudah lewat / berubah kondisi saat automatrade baru sempat scan,
# padahal signal-only sempat menangkapnya di menit yang sama.

# ── Pair List ───────────────────────────────────────────────────────────────
# FIX (Aug 2026): PAIR_LIST sempat dipangkas balik ke 47 pair persis sesuai
# SIGNALONLY, karena 106 pair bikin siklus scan AUTOTRADE molor dari ~1 menit
# ke ~10-11 menit (dibagi ke MAX_WORKERS yang sama), sehingga sinyal
# SIGNALONLY sering basi saat giliran AUTOTRADE baru sempat scan lagi.
#
# UPDATE 1 (Aug 2026): 58 pair top market-cap ditambahkan lagi ke AUTOTRADE
# SAJA — total 105 pair, 1:1 dengan SIGNALONLY sengaja dilepas. Diverifikasi
# lewat log real: WLDUSDT di AUTOTRADE cuma sempat discan 12x dalam ~13 jam
# (~9.5 menit/siklus rata-rata) vs SIGNALONLY yang scan tiap ~70 detik —
# AUTOTRADE melewatkan window BEARISH valid yang sempat tertangkap
# SIGNALONLY karena telat scan, BUKAN karena beda logika/algoritma.
#
# UPDATE 2 (Aug 2026, request user): Disamakan 1:1 LAGI ke SIGNALONLY versi
# terbaru (main__15_.py, "v19 Demand & Supply") — 86 pair. PAIR_LIST di bawah
# di-generate dari variabel PAIRS di SIGNALONLY tsb (format "BTC/USDT" →
# "BTCUSDT"). CATATAN: 86 pair masih jauh di atas 47 pair "aman" dari fix
# pertama — masalah scan cycle lambat BELUM tentu selesai hanya dengan
# menurunkan dari 105 ke 86. MAX_WORKERS masih 6, SCAN_INTERVAL masih 60s.
# Kalau WLD/pair lain masih sering kelewatan, pertimbangkan naikkan
# MAX_WORKERS atau audit ulang seberapa berat tiap pair discan.
PAIR_LIST = [
    "1000BONKUSDT", "1000FLOKIUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "1INCHUSDT",
    "AAVEUSDT",     "ADAUSDT",       "ALGOUSDT",      "APTUSDT",      "ARBUSDT",
    "ARKMUSDT",     "ARUSDT",        "ATOMUSDT",      "AVAXUSDT",     "AXSUSDT",
    "BCHUSDT",      "BLURUSDT",      "BNBUSDT",       "BTCUSDT",      "CHZUSDT",
    "COMPUSDT",     "CRVUSDT",       "DOGEUSDT",      "DOTUSDT",      "DYDXUSDT",
    "EGLDUSDT",     "ENAUSDT",       "ENSUSDT",       "ETCUSDT",      "ETHUSDT",
    "FETUSDT",      "FILUSDT",       "FLOWUSDT",      "FTMUSDT",      "GALAUSDT",
    "GMXUSDT",      "GRTUSDT",       "HBARUSDT",      "HYPEUSDT",     "ICPUSDT",
    "IDUSDT",       "IMXUSDT",       "INJUSDT",       "JTOUSDT",      "JUPUSDT",
    "KASUSDT",      "LDOUSDT",       "LINKUSDT",      "LTCUSDT",      "MAGICUSDT",
    "MANAUSDT",     "MASKUSDT",      "MEMEUSDT",      "MKRUSDT",      "NEARUSDT",
    "NOTUSDT",      "ONDOUSDT",      "OPUSDT",        "ORDIUSDT",     "PENDLEUSDT",
    "POLUSDT",      "PYTHUSDT",      "QNTUSDT",       "RENDERUSDT",   "RSRUSDT",
    "RUNEUSDT",     "SANDUSDT",      "SEIUSDT",       "SNXUSDT",      "SOLUSDT",
    "STRKUSDT",     "STXUSDT",       "SUIUSDT",       "TAOUSDT",      "THETAUSDT",
    "TIAUSDT",      "TONUSDT",       "TRXUSDT",       "UNIUSDT",      "WIFUSDT",
    "WLDUSDT",      "WUSDT",         "XLMUSDT",       "XRPUSDT",      "ZILUSDT",
    "ZRXUSDT",
]




# ── Price Action + Supply & Demand + S/R Detection Parameters ───────────────
SWING_WINDOW          = 5        # candle kiri/kanan untuk swing high/low

# ── FILTER STRUKTUR LTF (v-struct 2026-11-17) ─────────────────────────────────
# Gate BOS lama (_detect_ltf_bos) cuma cek "ada close di atas/bawah high/low 10 candle terakhir" — bounce kecil
# di tengah tren lawan bisa lolos. Filter ini menuntut struktur searah di entry TF:
#   LONG : swing high terbaru > sebelumnya (HH) DAN swing low terbaru > sebelumnya (HL),
#          dan belum ada close di bawah HL terakhir (belum CHoCH).
#   SHORT: kebalikannya (LH + LL, belum ada close di atas LH terakhir).
# False = perilaku lama. Di backtest bisa dinyalakan lewat token Telegram `sf1` (tanpa ganti file).
STRUCT_FILTER_LTF     = False
SR_LOOKBACK           = 80       # candle lookback untuk deteksi S/R level
SR_TOUCH_MIN          = 2        # minimum touch agar level valid
SR_PROXIMITY_PCT      = 0.003    # 0.3% proximity — harga dianggap "di zona S/R"
ZONE_LOOKBACK         = 60       # candle lookback untuk Supply/Demand zone
ZONE_MITIGATED_LIMIT  = 4        # max berapa kali zona boleh disentuh sebelum invalid
                                 # v26: dinaikkan 2→4 agar zona proven (multi-touch tapi hold)
                                 # tidak langsung dibuang — zona seperti ICP demand April valid
ZONE_PROXIMITY_PCT    = 0.005    # 0.5% toleransi price di dalam zona
MIN_ZONE_STRENGTH_PCT = 0.20     # impulse minimal 0.20% untuk zone valid

# ── Breakout + Retest Detection (dipakai tier SIMPLE / mode BOS_FVG_4H) ─────
# Gantiin sistem skor: cuma 3 gate — breakout valid (body close, bukan wick),
# retest ke zona breaker OB dari leg breakout itu sendiri, + candle konfirmasi.
BOS_BREAKOUT_LOOKBACK   = 30      # candle 4H terakhir untuk cari break yang masih segar (~5 hari)
BOS_BREAKER_SEARCH_BACK = 15      # max candle mundur dari break candle untuk cari breaker OB

# ── Proven Daily Zone (PDZ) ───────────────────────────────────────────────────
# Zona Daily yang sudah disentuh 2–5x dan masih hold = zona terbukti terkuat.
# Entry konfirmasi tetap di TF kecil, tapi zona ini kasih bonus score besar.
DAILY_ZONE_LOOKBACK         = 200    # candle 1D lookback (~200 hari)
DAILY_PROVEN_TOUCH_MIN      = 2      # minimum sentuhan agar dianggap "proven"
DAILY_PROVEN_TOUCH_MAX      = 6      # > ini dianggap sudah terlalu lelah / mitigated
DAILY_PROVEN_ZONE_PROX_PCT  = 0.015  # 1.5% toleransi — zona Daily lebih lebar
SCORE_DAILY_PROVEN_ZONE     = 20     # bonus poin untuk harga dekat proven daily zone
SCORE_DAILY_PROVEN_ZONE_EXT = 8      # bonus tambahan jika zona Daily + OB/FVG LTF tumpuk
WICK_BODY_RATIO_MIN   = 1.5      # pin bar: wick harus >= 1.5x body
ATR_PERIOD            = 14
PA_LOOKBACK           = 5        # candle terakhir untuk cek Price Action pattern

# ── Scoring Weights (PA + S&D + S/R Flow) ────────────────────────────────────
# HTF Bias
SCORE_HTF_ALIGNED     = 15       # HTF trend sejalan dengan arah trade (disamakan signal bot)
SCORE_HTF_PENALTY     = -10      # HTF berlawanan arah

# Price Action Pattern (pin bar, engulfing, inside bar breakout)
SCORE_PA_STRONG       = 25       # engulfing (sinyal reversal/continuation kuat)
SCORE_PA_MEDIUM       = 15       # pin bar (rejection dari level)
SCORE_PA_WEAK         = 8        # inside bar breakout (compression → expansi)
SCORE_PA_NONE         = -5       # tidak ada PA pattern

# Supply & Demand Zone
SCORE_SD_FRESH        = 20       # zona fresh (belum pernah disentuh / baru 1x)
SCORE_SD_TESTED       = 10       # zona sudah pernah disentuh 1x
SCORE_SD_NONE         = 0        # tidak ada zona S&D valid di sekitar harga

# Support & Resistance Level
SCORE_SR_STRONG       = 15       # harga di dekat S/R dengan ≥3 touches
SCORE_SR_MEDIUM       = 8        # harga di dekat S/R dengan 2 touches
SCORE_SR_NONE         = 0        # tidak ada S/R dekat

# Volume confirmation
SCORE_VOLUME          = 5        # ALIGNED WITH BASE (sebelumnya dinaikkan ke 8)
# Session
SCORE_SESSION         = 5
SCORE_SESSION_ASIA    = -3       # Asia session: altcoin lebih choppy, spread lebih lebar → penalti ringan
# Macro
SCORE_MACRO_ALIGNED   = 8
SCORE_MACRO_CONFLICT  = -8    # ALIGNED WITH BASE: dikembalikan ke -8 (sebelumnya v28 dinaikkan ke -15)
BTC_H4_GATE_PENALTY   = -30   # 2026-09-09: dinaikkan dari -15 — data 24 trade (21 LONG/2 SHORT,
                               # 11/21 LONG entry saat btc_bias BEARISH, mayoritas SL) menunjukkan
                               # -15 tidak cukup menahan sinyal LONG skor tinggi (70-90) saat BTC turun
BTC_SHORT_BIAS_BONUS  = 10    # 2026-09-09: bonus skor SHORT saat BTC 4H BEARISH — sebelumnya
                               # hanya penalty asimetris ke LONG, SHORT tidak dapat keuntungan aktif
                               # dari kondisi macro yang justru mendukungnya

SIGNAL_MODE           = "NORMAL"
MIN_SCORE_FULL_NORMAL = 55   # disamakan dengan signal bot (21)
MIN_SCORE_FULL_SNIPER = 75   # ALIGNED WITH BASE (signal-only) — disamakan 1:1
# 2026-11-10: dinaikkan lagi 40 → 50. Backtest 30 hari (15 pair, 173 trade): bucket
# skor 40-49 = 32 trade, WR 15.6%, PnL -201 USDT (hampir seluruh kerugian total
# -305), semuanya LONG. Penurunan 10-02 (40) terbukti menambah trade jelek.
MIN_SCORE_RELAXED     = 50
MIN_SCORE = MIN_SCORE_FULL_NORMAL if SIGNAL_MODE == "NORMAL" else MIN_SCORE_FULL_SNIPER

# FIX (2026-09-03, 1:1 PORT dari base): backtest 64 trade tier RELAXED — SHORT
# WR 19% (27/35 total SL adalah SHORT) vs LONG WR 45%. SHORT di RELAXED harus
# lolos gate dengan skor lebih tinggi (efektif dikurangi penalty ini) supaya
# tidak asal entry melawan bias market yang sedang dominan.
# 2026-11-10: penalti SHORT 10 → 0. Penalti ini lahir dari satu rezim (Sep: SHORT
# WR 19% vs LONG 45%); di Okt-Nov kebalikannya (SHORT 48.5% vs LONG 15.7%). Skor
# harus simetris — arah yang cocok ditentukan oleh TREND GATE di analyze_pair,
# bukan oleh penalti arah yang bergantung rezim. Karena floor naik ke 50 dan
# penalti jadi 0, SHORT tetap butuh skor mentah >= 50 (sama seperti sebelumnya).
SHORT_SCORE_PENALTY_RELAXED = 0

# 1:1 PORT dari base — dipakai _pick_sl_swing()/_swing_strength() untuk memilih
# SL dari swing struktural terkuat (bukan sekadar swing terdekat), dengan cap
# jarak maksimum dari entry.
SL_MAX_ATR_MULT       = 2.5
SL_SWING_OB_TOLERANCE = 0.006  # toleransi overlap swing↔OB (0.6%) untuk cek "swing di dalam OB"

# ── Market Breadth Filter (v28) ───────────────────────────────────────────────
# Hitung arah mayoritas sinyal dalam satu scan cycle.
# Jika mayoritas LONG → SHORT berlawanan market → diblok (dan sebaliknya).
# Breadth dihitung dari direction semua signal_candidates setelah scan.
BREADTH_ENABLED        = False  # ALIGNED WITH BASE: nonaktif (catatan: get_market_breadth() tidak pernah dipanggil di alur eksekusi manapun, jadi tidak pernah aktif secara fungsional)
BREADTH_THRESHOLD      = 0.60
BREADTH_MIN_SAMPLES    = 4

# ── Counter-Trend Momentum Gate (v28) ─────────────────────────────────────────
# Sinyal berlawanan market (SHORT saat mayoritas LONG, atau sebaliknya) hanya boleh
# lolos jika setup-nya benar-benar sangat kuat: RR ≥ grade A per mode.
# Tujuan: SHORT nyempil saat alt season harus punya RR sangat bagus, bukan seadanya.
COUNTER_TREND_GATE_ENABLED = False  # ALIGNED WITH BASE: nonaktif (catatan: sebelumnya juga tidak pernah dipanggil di manapun, jadi tidak pernah aktif secara fungsional)
COUNTER_TREND_MIN_RR       = 3.5   # tidak dipakai selama gate di atas False

# ── Custom Score Override via /setscoreupto ──────────────────────────────────
# Jika > 0, nilai ini OVERRIDE MIN_SCORE (FULL tier) DAN MIN_SCORE_RELAXED (RELAXED tier).
# PENTING: RELAXED tier (LOW_TF/LTF_30M) juga kena gate yang SAMA — tidak ada diskon 60%.
# Set ke 0 untuk kembali ke default scoring threshold.
# Contoh: /setscoreupto 60  → semua tier (FULL & RELAXED) wajib score ≥ 60
MIN_SCORE_CUSTOM: int          = 55  # disamakan dengan signal bot (21)
MIN_SCORE_RELAXED_CUSTOM: int  = 50  # 2026-11-10: 40 → 50 (lihat MIN_SCORE_RELAXED)

RR_MIN               = 1.5   # minimum RR — swing struktural terdekat yang realistis
RR_GRADE_A           = 2.0   # disamakan dengan signal bot — full parity analyze_pair
# 2026-11-10: TP1 dipindah 1.5R → 0.7R (partial 50% + SL ke BE, TP2 tetap 2.5R).
# Diagnosa dari 173 + 139 trade backtest: harga hanya mencapai +1.5R sebelum -1R
# di 35-38% trade (acak murni ≈ 40%), dan +0.5R di 58-61% (acak murni ≈ 67%) —
# entry tidak punya edge ke arah TP jauh. TP1 dekat membuat sebagian besar trade
# ditutup profit/BE, bukan SL. Simulasi ulang exit dari MFE tiap trade (model
# cocok persis dengan hasil asli): winrate 38% → ~53%, expectancy -0.10R → -0.05R
# per trade. RR_GRADE_B dipakai konsisten sebagai (a) pengali TP1 di calculate_rr,
# (b) batas minimal RR1 di semua guard order/notif, jadi cukup ubah nilai ini.
# Untuk kembali ke perilaku lama: TP1_R_MULT = 1.5
TP1_R_MULT           = 0.7
# Buffer SL di bawah/atas zona = SL_BUFFER_ATR_MULT × ATR. Nilai 1.2 = perilaku lama
# (dinaikkan 0.85→1.2 pada 2026-09-03 karena 55% trade kena SL oleh noise wick).
SL_BUFFER_ATR_MULT   = 1.2
RR_GRADE_B           = TP1_R_MULT   # Grade B = threshold minimum (= pengali TP1)
if SINGLE_TP_MODE:
    # Mode 1-TP: floor RR (guard fill-adjust, clamp, notif) = RR TP tunggal, min 1.5
    SINGLE_TP_R_MULT = max(SINGLE_TP_R_MULT, 1.5)
    RR_GRADE_B       = SINGLE_TP_R_MULT

# RR per mode — scalping TF kecil pakai threshold lebih longgar
RR_MIN_PER_MODE: dict = {
    "LOW_TF"    : 1.5,   # 15m entry
    "HTF_4H"    : 1.5,   # HTF bias 4h, entry TF 1h, tier FULL
    "LTF_30M"   : 1.5,   # 30m entry
    "SCALPING"  : 1.5,   # 1h entry
    "INTRADAY"  : 1.5,   # 1h HTF entry
    "GOLD_SCALP": 1.5,   # 5m entry Gold — RR 1.5 minimum, SL sempit jadi TP realistis
}
RR_GRADE_A_PER_MODE: dict = {
    "LOW_TF"    : 2.0,
    "HTF_4H"    : 2.0,
    "LTF_30M"   : 2.0,
    "SCALPING"  : 2.5,
    "INTRADAY"  : 2.5,
    "GOLD_SCALP": 2.0,   # Grade A Gold: RR 2.0 — TP 1% dengan SL 0.5%
}

def get_rr_min_for_mode(mode_label: str) -> float:
    """
    Return RR minimum untuk mode tertentu.
    Saat Super Scalper Mode ON:
      → RR 1.8 untuk LOW_TF/LTF_30M.
      Alasan: SL diperketat ke 1.2-1.5%, sehingga TP target 2.2-2.7% masih sangat
      realistis dalam 2-4 candle 15m/30m di coin high-liquidity.
      RR 1.2 terlalu longgar (TP cuma 1.44% dengan SL 1.2% = hampir tidak worth).
    """
    if _SUPER_SCALPER_MODE:
        return 1.8
    return RR_MIN_PER_MODE.get(mode_label, RR_MIN)

RELAXED_VOL_RATIO_MIN = 1.8

# ── Macro ───────────────────────────────────────────────────────────────────
BTC_BIAS_TF        = "1h"   # ALIGNED WITH BASE (signal-only) — disamakan 1:1

# ── BTC + BTC.D Correlation Filter (/togglebtcfilter) ───────────────────────
#
# Filter ini menggabungkan arah BTC price dan BTC Dominance secara bersamaan
# untuk menentukan sinyal apa yang boleh lolos (altcoin vs BTCUSDT).
#
# Tabel kombinasi (altcoin):
# ┌─────────────┬──────────────┬──────────────────────────────────────────┐
# │  BTC price  │   BTC.D      │  Signal yang diizinkan (altcoin)         │
# ├─────────────┼──────────────┼──────────────────────────────────────────┤
# │  BULLISH    │  BEARISH     │  LONG altcoin  ✅  (altcoin season)      │
# │  BULLISH    │  BULLISH     │  Ambiguous — SKIP (BTC rally, dom naik)  │
# │  BULLISH    │  RANGING     │  LONG altcoin  ✅  (alt ikut BTC naik)   │
# │  BEARISH    │  BULLISH     │  SHORT altcoin ✅  (BTC turun dom naik)  │
# │  BEARISH    │  BEARISH     │  Ambiguous — SKIP (BTC turun dom turun)  │
# │  BEARISH    │  RANGING     │  SHORT altcoin ✅  (alt ikut BTC turun)  │
# │  RANGING    │  BEARISH     │  LONG altcoin  ✅  (BTC sideways, dom ↓) │
# │  RANGING    │  BULLISH     │  SHORT altcoin ✅  (BTC sideways, dom ↑) │
# │  RANGING    │  RANGING     │  SKIP (tidak ada arah jelas)             │
# └─────────────┴──────────────┴──────────────────────────────────────────┘
#
# Untuk BTCUSDT sendiri: hanya pakai arah BTC price (dom tidak relevan).
#
# Default: OFF agar tidak mengubah perilaku bot yang sudah ada.
BTC_CORR_FILTER_ON: bool = True    # toggle via /togglebtcfilter

# BTC.D timeframe untuk deteksi trend dominance (BTCDOMUSDT, pakai LTF)
BTCD_TF: str = "15m"

# ── Cooldown ────────────────────────────────────────────────────────────────
COOLDOWN_MINUTES = 10
COOLDOWN_AFTER_CLOSE = 0    # 0 = tidak ada cooldown setelah posisi close (TP)
COOLDOWN_AFTER_SL_HOURS = 3  # FIX: dikurangi dari 8 → 3 jam, hindari miss reversal setelah SL
_cooldown_map: dict = {}


# ── Cooldown Functions ───────────────────────────────────────────────────────

def is_on_cooldown(pair: str, direction: str) -> tuple:
    """
    Cek apakah pair+direction sedang dalam cooldown sinyal.
    Return: (on_cooldown: bool, remaining_minutes: float, reason: str)
    
    Cooldown per arah (BULLISH/BEARISH) — arah berlawanan tetap boleh masuk.
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)

    # Cek cooldown spesifik arah (signal cooldown)
    key_dir = f"{pair}|{direction}"
    until_dir = _cooldown_map.get(key_dir)
    if until_dir and now < until_dir:
        remaining = (until_dir - now).total_seconds() / 60
        dir_str = "LONG" if direction == "BULLISH" else "SHORT"
        return True, remaining, f"Cooldown {dir_str} — sisa {remaining:.0f} menit"

    # 1:1 DENGAN BASE: cooldown post-close / post-SL / blacklist SL (key
    # "{pair}|CLOSE", blok SEMUA arah) DIHAPUS dari keputusan sinyal. Base hanya
    # punya cooldown per-arah setelah sinyal dikirim. (Base memang punya
    # shorten_cooldown setelah SL, tapi itu hanya MEMPERSINGKAT cooldown arah
    # — tidak pernah memblok arah lawan.)
    return False, 0.0, ""


def set_cooldown(pair: str, label: str, entry_tf: str = "", direction: str = None):
    """
    Set cooldown sinyal setelah trade dieksekusi.
    Menggunakan SIGNAL_COOLDOWN_HOURS untuk durasi.

    ALIGNED WITH BASE — FIX BUG: sebelumnya fungsi ini SELALU nge-set cooldown
    untuk KEDUA arah (BULLISH dan BEARISH) tiap kali ada sinyal, padahal BASE
    cuma cooldown arah yang barusan fire (arah berlawanan tetap boleh masuk).
    Akibatnya pair yang baru dapat sinyal LONG jadi diblok total dari SHORT
    juga selama SIGNAL_COOLDOWN_HOURS — jauh lebih ketat dari BASE dan salah
    satu penyebab signifikan sinyal jadi lebih sedikit dari BASE.
    """
    from datetime import timedelta
    now   = datetime.now(timezone.utc)
    until = now + timedelta(hours=SIGNAL_COOLDOWN_HOURS)

    key_label = f"{pair}|{label}"
    _cooldown_map[key_label] = until
    if direction in ("BULLISH", "BEARISH"):
        _remember_pos_direction(pair, "LONG" if direction == "BULLISH" else "SHORT")

    if direction in ("BULLISH", "BEARISH"):
        _cooldown_map[f"{pair}|{direction}"] = until
        print(f"  ⏳ Cooldown set: {pair} [{label}] {direction} sampai {until.strftime('%H:%M')} UTC")
    else:
        # Fallback kalau direction tidak di-pass (backward compat) — set kedua
        # arah seperti perilaku lama, supaya tidak crash caller yang belum diupdate.
        _cooldown_map[f"{pair}|BULLISH"] = until
        _cooldown_map[f"{pair}|BEARISH"] = until
        print(f"  ⏳ Cooldown set: {pair} [{label}] sampai {until.strftime('%H:%M')} UTC (arah tidak diketahui, blok keduanya)")


SHORT_COOLDOWN_AFTER_SL_HOURS = 2   # 1:1 dgn base signal-only (shorten_cooldown)
_last_pos_direction: dict = {}      # symbol -> "LONG"/"SHORT" (arah posisi terakhir)


def _remember_pos_direction(symbol: str, direction: str):
    """Rekam arah posisi terakhir per symbol (dipakai saat close)."""
    if direction in ("LONG", "SHORT"):
        _last_pos_direction[symbol] = direction


def set_cooldown_post_close(pair: str, reason: str = "", is_stoploss: bool = False):
    """
    1:1 DENGAN BASE (signal-only main46, record_closed_trade):
      • TIDAK ada lagi cooldown "{pair}|CLOSE" yang memblok SEMUA arah
        (COOLDOWN_AFTER_SL_HOURS 3j) — base tidak punya itu.
      • Saat SL / Break-Even / EXPIRED / FLIP, base MEMPERSINGKAT cooldown arah
        posisi itu jadi SHORT_COOLDOWN_AFTER_SL_HOURS (2 jam) via
        shorten_cooldown() — bukan menambah blok baru. Ditiru di sini.
      • Close karena TP2/manual: cooldown arah dibiarkan (12 jam penuh), sama
        seperti base.
    Pemanggil hanya mengirim symbol; arah diambil dari _last_pos_direction.
    """
    from datetime import timedelta
    # bersihkan sisa key CLOSE lama (mis. dari state lama) supaya tidak memblok
    _cooldown_map.pop(f"{pair}|CLOSE", None)

    _r = (reason or "").lower()
    # SL murni, atau break-even (trailing BE), atau structure-reversal (=FLIP)
    _shorten = bool(is_stoploss) or ("break" in _r and "even" in _r) \
               or "trailing be" in _r or "structure reversal" in _r
    if not _shorten:
        print(f"  ✅ {pair} close ({reason}) — cooldown arah tidak diubah (1:1 base)")
        return

    _dir = _last_pos_direction.get(pair)
    if _dir not in ("LONG", "SHORT"):
        print(f"  ⚠️  {pair} arah posisi tidak diketahui — cooldown tidak dipersingkat")
        return
    _bd = "BULLISH" if _dir == "LONG" else "BEARISH"
    _key = f"{pair}|{_bd}"
    _remaining_as_new = SIGNAL_COOLDOWN_HOURS - SHORT_COOLDOWN_AFTER_SL_HOURS
    if _remaining_as_new <= 0:
        _cooldown_map.pop(_key, None)
    else:
        # base memundurkan timestamp mulai supaya sisa cooldown = 2 jam.
        # Di sini _cooldown_map menyimpan WAKTU BERAKHIR, jadi = now + 2 jam,
        # tapi hanya jika itu lebih PENDEK dari yang ada (tidak pernah memperpanjang).
        _new_until = datetime.now(timezone.utc) + timedelta(hours=SHORT_COOLDOWN_AFTER_SL_HOURS)
        _cur = _cooldown_map.get(_key)
        if _cur is None or _new_until < _cur:
            _cooldown_map[_key] = _new_until
    print(f"  🔓 Cooldown {_key} dipersingkat jadi {SHORT_COOLDOWN_AFTER_SL_HOURS}j "
          f"({reason or 'SL/BE/FLIP'}) — 1:1 base")


# ── fetch_ohlcv: alias ke fetch_ohlcv_realdata (untuk kompatibilitas kode lama) ──
# Beberapa bagian kode masih memanggil fetch_ohlcv(pair, tf, limit=N)
# dengan format pair "BTC/USDT" (ccxt style) — perlu dikonversi ke "BTCUSDT"
# ── FIX ENTRY (v79): analisa hanya pakai candle TERTUTUP ─────────────────────
# Sebelumnya candle yang masih berjalan ikut dianalisa (BOS/sweep/zona/close
# "price" berubah-ubah sampai candle tutup → sinyal repaint, masuk di wick
# yang belum jadi). Backtest hanya pakai candle tertutup, jadi live ≠ backtest.
CLOSED_CANDLE_ONLY = True

def _drop_forming_candle(df):
    if not CLOSED_CANDLE_ONLY or df is None or len(df) < 52:
        return df
    try:
        ts = df["timestamp"]
        step = (ts.iloc[-1] - ts.iloc[-2]).total_seconds()
        if step <= 0:
            return df
        open_utc = ts.iloc[-1].to_pydatetime().replace(tzinfo=timezone.utc).timestamp()
        if time.time() < open_utc + step - 1:
            return df.iloc[:-1].reset_index(drop=True)
    except Exception:
        pass
    return df


def fetch_ohlcv(pair: str, timeframe: str, limit: int = 250) -> pd.DataFrame:
    """
    1:1 DENGAN main73 (signal-only): default limit=250 dan RAISE RuntimeError
    saat gagal / <50 candle (bukan return None). Dipakai oleh semua fungsi
    STRATEGI (analyze_pair, get_btc_bias, fetch_btc_ltf_data, fetch_btcd_ohlcv,
    fetch_all_tf_for_pair) supaya jalur error/fallback-nya sama persis dengan
    main73 (mis. fallback CoinGecko di fetch_btcd_ohlcv).
    Sumber data tetap fetch_ohlcv_realdata (Binance Futures REST, semaphore +
    retry 418/429 = sistem, dipertahankan).
    """
    symbol = pair.replace("/", "") if "/" in pair else pair
    df = fetch_ohlcv_realdata(symbol, timeframe, limit=limit)
    if df is None or len(df) < 50:
        raise RuntimeError(f"Binance failed for {pair} @ {timeframe}: "
                           f"{0 if df is None else len(df)} candles")
    return _drop_forming_candle(df)


def fetch_ohlcv_safe(pair: str, tf: str, limit: int = 200) -> pd.DataFrame | None:
    """Perilaku LAMA fetch_ohlcv (return None saat gagal) — khusus caller SISTEM
    (trailing, pending monitor, /status, regime info, dll) yang cek `is None`."""
    symbol = pair.replace("/", "") if "/" in pair else pair
    return fetch_ohlcv_realdata(symbol, tf, limit=limit)



# ── SMC Ranging Adaptive Config (dari main(11) Section 20) ──────────────────
LTF_RANGING_SCORE_GATE      = 40     # Minimum composite LTF score saat HTF ranging (1:1 reference bot)
LTF_CONSECUTIVE_MIN         = 3      # Jumlah candle berturut-turut arah sama
DISPLACEMENT_BODY_RATIO_MIN = 0.60   # Body / range candle displacement
VOLUME_SPIKE_MULTIPLIER     = 1.5    # Volume spike = N× avg 20 bar
RANGE_ATR_FACTOR            = 3.0    # Range HTF minimal N× ATR agar layak trade
RANGE_EDGE_BUFFER_PCT       = 0.15   # Buffer 15% dari tepi range
STRONG_BOS_BODY_PCT         = 0.40   # Body candle BOS >= 40% = strong
SESSION_BONUS_MAP           = {"London": 15, "New York": 15, "Asia": 5, "Off-Hours": 0}

# ── SMC Macro Config ────────────────────────────────────────────────────────
# FIX: bot ini pakai format pair TANPA slash (mis. "BTCUSDT", dari PAIR_LIST),
# beda dengan main.py (signal-only) yang pakai "BTC/USDT". Set lama copy-paste
# dari main.py jadi tidak pernah match → BTC/ETH gagal ke-exempt dari Market
# Regime Block & BTC Multi-TF Guard. Disamakan ke format pair asli bot ini.
ALTCOIN_EXEMPTIONS    = {"BTCUSDT", "ETHUSDT"}
BTCD_SMA_PERIOD       = 20
ATR_MIN_PCT           = 0.15
CONFIRM_BODY_RATIO    = 0.55
MAX_POSITION_HOURS    = 72
SIGNAL_HASH_TTL_HOURS = 24   # hash dedup: 24 jam (ALIGNED WITH BASE)
SIGNAL_COOLDOWN_HOURS = 12  # disamakan dengan bot signal-only

# ── SMC Correlation Groups ───────────────────────────────────────────────────
# 1:1 PARITY FIX (2026-09-18): sebelumnya masih format dengan slash
# ("DOGE/USDT") copy-paste dari signal.py, padahal pair internal bot ini TANPA
# slash ("DOGEUSDT", lihat PAIR_LIST). Akibatnya _get_correlation_group()
# ("if pair in group") tidak PERNAH match apapun — apply_correlation_filter()
# jadi silent no-op, filter korelasi mati total meski dipanggil. Isi grup
# tetap sama persis dengan signal.py, cuma format string disamakan ke pair
# internal bot ini (tanpa slash) supaya benar-benar match & berfungsi.
CORRELATION_GROUPS = [
    {"DOGEUSDT", "1000SHIBUSDT", "1000PEPEUSDT", "GALAUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT", "CHZUSDT"},
    {"ETHUSDT",  "ARBUSDT",  "OPUSDT",   "IMXUSDT"},
    {"SOLUSDT",  "APTUSDT",  "SUIUSDT",  "NEARUSDT"},
    {"AVAXUSDT", "ATOMUSDT", "DOTUSDT",  "EGLDUSDT"},
    {"LINKUSDT", "GRTUSDT",  "RENDERUSDT"},
    {"BNBUSDT",  "INJUSDT"},
]

# ── SMC Strategy Detection Parameters (dari main(11)) ───────────────────────
SWEEP_LOOKBACK       = 30
OB_LOOKBACK          = 40
FVG_LOOKBACK         = 40
EQH_EQL_TOLERANCE    = 0.0015
REJECTION_TOLERANCE  = 0.003

# ── TP vs Liquidity Target (permintaan user) ────────────────────────────────
# Masalah nyata: TP dihitung murni dari RR ratio (entry + sl_dist * rr), jadi
# posisinya di harga acak yang gak ada hubungannya dengan struktur — kalau
# kebetulan ada liquidity level (swing high/low lama, equal high/low) PERSIS
# di dekat situ, harga sering REJECT SEBELUM sampai TP murni RR (karena price
# "mencari" liquidity itu buat disapu duluan, lalu reversal) — kasus nyata:
# COMPUSDT LONG entry 20.56, TP RR di 21.14, tapi liquidity/swing high lama
# ada di ~21.20, harga cuma sampai situ lalu reject turun, TP RR di 21.14
# TIDAK PERNAH ke-hit walau cuma selisih tipis.
# Fix: kalau ada liquidity level (dari detect_eqh_eql / swing high-low, fungsi
# yang SAMA dipakai detect_liquidity_sweep untuk entry) di ANTARA entry dan TP
# hasil RR, tarik TP mundur sedikit di BAWAH level itu (LIQUIDITY_TP_BUFFER_PCT)
# — bukan pas di levelnya, karena reject biasanya mulai SEBELUM full-touch.
# TP hasil RR murni tetap dipakai sebagai fallback/ceiling kalau tidak ada
# liquidity level yang relevan ditemukan.
LIQUIDITY_TP_ADJUST_ENABLED = True
LIQUIDITY_TP_BUFFER_PCT     = 0.0015  # tarik TP ke (level - 0.15%) untuk LONG, (level + 0.15%) untuk SHORT

OB_MITIGATION_LIMIT  = 2
MIN_DISPLACEMENT_PCT = 0.25

# ── Entry dekat market kalau OB sudah ter-retest + ada rejection candle ────
# Permintaan user: kalau zona OB sudah pernah disentuh (taps>=1, bukan lagi
# "perawan") DAN ada rejection candle valid (pin bar/engulfing/inside break,
# lihat detect_rejection_candle) di entry TF dekat harga sekarang, entry
# TIDAK perlu mundur jauh ke zone["mid"] — reject di level itu sudah jadi
# konfirmasi tambahan, jadi entry dipasang dekat harga rejection candle.
# RR tetap dicek lewat gate RR_MIN_PER_MODE yang sudah ada — kalau entry
# lebih dekat bikin RR jelek, sinyal tetap kena skip di situ, bukan di sini.
REJECT_ENTRY_NEAR_MARKET_ENABLED = True
REJECT_ENTRY_MIN_TAPS            = 1   # OB minimal sudah ke-tap sekali (sudah pernah ditest)

# ── GUARD JARAK ENTRY vs MARKET (permintaan user) ───────────────────────────
# Masalah: kalau OB/FVG terpilih jauh dari market_price, entry = zone["mid"]
# jadi jauh di bawah (LONG) / atas (SHORT) harga sekarang. Sinyal tetap
# dikirim + limit order dipasang, tapi peluang harga pullback SEDALAM itu
# sebelum keburu ke TP duluan kecil — order sering auto-cancel (TP kesentuh
# duluan tanpa fill). Efek sampingnya: sinyal yang "kejauhan" ini tetap
# terkirim/makan slot notifikasi & bandwidth analisa, padahal probabilitas
# fill-nya rendah — sementara pair lain yang OB-nya SUDAH dekat market
# (kasus nyata: COMPUSDT, fill di 09:51 lalu lanjut profit) tidak dibedakan
# dari yang jauh.
# Fix: kalau jarak entry ke market_price melebihi MAX_ENTRY_DISTANCE_PCT,
# JANGAN kirim sinyal sekarang — skip dulu (bukan reject permanen). Begitu
# harga sudah mendekat di scan berikutnya, entry akan otomatis lebih dekat
# (OB/struktur sama, market_price yang bergerak), dan sinyal baru bisa lolos
# gate ini lalu terkirim dengan peluang fill yang lebih realistis.
MAX_ENTRY_DISTANCE_ENABLED = True
MAX_ENTRY_DISTANCE_PCT     = 0.006   # 0.6% — di atas ini, entry dianggap "kejauhan", skip dulu
ENTRY_REQUIRE_ZONE        = True    # v79: wajib di zona OB/FVG — tanpa zona = "entry market" tanpa edge, skip
MIN_FVG_PCT          = 0.08
RSI_PERIOD           = 14
RSI_OVERBOUGHT       = 70   # LONG diblok jika RSI ≥ nilai ini (overbought)
RSI_OVERSOLD         = 30   # SHORT diblok jika RSI ≤ nilai ini (oversold)
EMA_PERIODS          = [20, 50, 200]

# ── RSI Position Gate Parameters — dipakai calculate_rsi_series() ───────────
# CATATAN: parameter RSI_GATE_* di bawah ini SUDAH TIDAK DIPAKAI lagi oleh
# gate utama (STOCH_POSITION_GATE_ENABLED) — gate itu sekarang balik pakai
# Stochastic asli (calculate_stochastic + detect_stoch_divergence, lihat
# STOCH_* di bawah), bukan RSI. Konstanta ini dibiarkan (tidak dihapus) untuk
# jaga-jaga kalau ada bagian lain yang masih reference calculate_rsi_series().
RSI_GATE_OVERSOLD      = RSI_OVERSOLD      # RSI < ini dianggap oversold (pakai constant existing, 30)
RSI_GATE_OVERBOUGHT    = RSI_OVERBOUGHT    # RSI > ini dianggap overbought (pakai constant existing, 70)
RSI_GATE_REVERSAL_UP   = 45   # LONG: RSI wajib naik balik melewati level ini setelah oversold
RSI_GATE_REVERSAL_DOWN = 55   # SHORT: RSI wajib turun balik melewati level ini setelah overbought
RSI_GATE_LOOKBACK      = 8    # Jumlah candle ke belakang untuk cek "pernah overbought/oversold"

# ── Stochastic Slow (5,3,3) Parameters — dipakai calculate_stochastic() ──────
# (terpisah dari StochRSI calc_stoch_rsi() yang sudah ada — ini stochastic
# klasik dari harga, dipakai fetch_btc_setup_data() & detect_stoch_divergence())
STOCH_K_PERIOD       = 5    # %K lookback
STOCH_D_PERIOD       = 3    # %D smoothing (SMA of %K)
STOCH_SMOOTH         = 3    # Slow %K smoothing
STOCH_OVERSOLD       = 20   # %K/%D di bawah ini → oversold
STOCH_OVERBOUGHT     = 80   # %K/%D di atas ini → overbought
STOCH_REVERSAL_ZONE  = 30   # Zona konfirmasi reversal bullish (K naik dari <20 ke >30)
STOCH_LOOKBACK       = 5    # Jumlah candle ke belakang untuk cek "pernah overbought/oversold"
                            # (dipakai STOCH_POSITION_GATE — lihat calculate_stochastic())

# ── Stochastic Gate (hard gate per sinyal) — /togglestochgate ───────────────
# v4: dikembalikan ke Stochastic asli (bukan RSI). Dua filter tambahan:
#   1. Divergence wajib ada searah sinyal — dicek di pair (entry TF) ATAU BTC.
#   2. Anti-berlawanan — skip kalau Stoch (pair ATAU BTC) sudah mentok di
#      zona ekstrem LAWAN arah sinyal dan mulai crossing balik ke arah lawan
#      itu (lihat stoch_opposing_reversal()).
# Bukan gate timing (tidak nunggu cross searah dulu) — cuma filter tambahan
# di atas structure. Nama variabel dipertahankan STOCH_POSITION_GATE_ENABLED
# supaya command /togglestochgate & save_state() lama tetap kompatibel.
# Default: ON.
STOCH_POSITION_GATE_ENABLED = True   # True = aktif | False = disable gate ini

# ── BTC Setup Guard — dipakai fetch_btc_setup_data() ─────────────────────────
# Semua sinyal altcoin harus tunggu BTC reversal terkonfirmasi (1:1 reference bot)
BTC_SETUP_TF         = "1h"   # Timeframe analisis setup BTC
BTC_SETUP_LTF        = "15m"  # LTF konfirmasi entry BTC
BTC_SUPPLY_LOOKBACK  = 50     # Lookback candle untuk deteksi supply/demand BTC

# ── IMB (Body Imbalance) & Gap Config ─────────────────────────────────────────
IMB_LOOKBACK         = 40    # Jumlah candle lookback untuk deteksi IMB
IMB_MIN_BODY_PCT     = 0.40  # Body candle impulsif minimal 40% dari range-nya
IMB_MIN_SIZE_PCT     = 0.06  # Ukuran IMB minimal 0.06% dari harga (cegah noise)
IMB_FILL_LIMIT       = 0.75  # IMB dianggap terisi jika >75% filled → skip
GAP_LOOKBACK         = 50    # Lookback untuk deteksi gap harga
GAP_MIN_SIZE_PCT     = 0.10  # Gap minimal 0.10% dari harga

# ── SMC Scoring Weights ──────────────────────────────────────────────────────
# Tier 2 — BOS / Market Structure
SCORE_BOS_STRONG     = 25
SCORE_BOS_WEAK       = 12
SCORE_BOS_PENALTY    = -10
# 2026-11-12: BOS NONE (tidak ada BOS di entry TF) jadi HARD GATE. Backtest
# backtest_log_20260812_0000_tg (66 trade): BOS NONE = 15 trade, 0 TP2, 9 SL,
# -77.4 USDT dari total -83.4. Tanpa BOS NONE sisanya ~-6 USDT (51 trade).
# Konsisten dengan temuan 08-27 (BOS-unconfirmed WR 8.3%). Penalti -10 saja
# tidak cukup karena trade masih lolos via score gate RELAXED (50).
# Set False untuk kembali ke perilaku lama (penalti skor saja).
BOS_NONE_HARD_GATE   = True

# Tier 3 — Momentum
SCORE_MOMENTUM       = 15
SCORE_MOMENTUM_NONE  = 0
SCORE_MOMENTUM_ANTI  = -5

# Tier 4 — Liquidity sweep/reaksi
SCORE_LIQUIDITY      = 15
SCORE_LIQUIDITY_WEAK = 8
SCORE_LIQUIDITY_NONE = -5

# Tier 5 — Entry Zone OB/FVG
SCORE_OB_AND_FVG     = 15
SCORE_OB_OR_FVG      = 10
SCORE_ZONE_NONE      = 0

# ── Zone Confluence Scoring ───────────────────────────────────────────────────
# Zona dianggap "kuat" jika ada beberapa faktor tumpuk di titik yang sama.
# Semakin banyak confluence → semakin besar kemungkinan mantul.
# Skor ini DITAMBAHKAN ke score total di atas entry_zone score.
# Maks teoritis: ~+40 pts (semua faktor terpenuhi sekaligus)
#
#   OB fresh (taps=0)          → +10  order di zona masih utuh, belum terisi
#   OB tested 1x               → +5   sudah teruji, masih relevan
#   FVG overlap OB             → +8   double imbalance — dua alasan harga balik
#   S/R tumpuk di zona         → +7   level historis ikut jadi magnet
#   Impulse kuat (>1%)         → +5   semakin cepat pasar tinggalkan = lebih banyak order tertinggal
#   Zona di Discount/Premium   → +5   beli di bawah eq (LONG) atau jual di atas eq (SHORT)
#   Super-fresh bonus          → +5   OB fresh + impulse tinggi sekaligus
#   Zona lemah (taps>2)        → -8   kemungkinan order di zona sudah habis
SCORE_ZONE_CONFLUENCE_OB_FRESH    = 10
SCORE_ZONE_CONFLUENCE_OB_TESTED   = 5
SCORE_ZONE_CONFLUENCE_FVG_OVERLAP = 8
SCORE_ZONE_CONFLUENCE_SR_OVERLAP  = 7
SCORE_ZONE_CONFLUENCE_IMPULSE     = 5
SCORE_ZONE_CONFLUENCE_PD_ARRAY    = 5
SCORE_ZONE_CONFLUENCE_SUPER_FRESH = 5
SCORE_ZONE_WEAK_PENALTY           = -8

# ── Premium/Discount/Equilibrium HARD GATE ─────────────────────────────────
# Sebelumnya P/D cuma bonus poin (+5) di fungsi yang ternyata TIDAK PERNAH
# DIPANGGIL (compute_zone_confluence_score, check_pre_zone_sniper — dead code,
# gak ada satupun caller-nya di file ini). Jalur live (analyze_pair) sama
# sekali tidak mempertimbangkan posisi entry relatif ke range Premium/
# Discount/Equilibrium, sehingga SHORT bisa masuk dari Equilibrium (RR jelek:
# SL jauh dari struktur, TP terlalu dekat karena entry sudah nempel zona
# target sendiri) — kasus QNTUSDT 23 Jul 2026.
#
# Gate ini WAJIB (bukan bonus): SHORT hanya lolos kalau entry ada di sepertiga
# atas range (Premium), LONG hanya lolos kalau entry di sepertiga bawah range
# (Discount). Entry di sepertiga tengah (Equilibrium) — direject untuk
# DUA arah, karena secara struktural gak ada sisi range yang diuntungkan.
# CATATAN: analyze_pair() sekarang SMC FLOW MURNI 1:1 reference bot — gate P/D
# ini TIDAK ADA padanannya di reference dan tidak dipanggil di jalur live mana
# pun lagi. Konstanta dibiarkan fixed OFF; variabel jadi dead code permanen.
PD_ZONE_HARD_GATE_ENABLED = False
PD_ZONE_LOOKBACK          = 50     # candle, buat hitung recent_high/recent_low
PD_ZONE_RATIO             = 1 / 3.0  # sepertiga atas = Premium, sepertiga bawah = Discount

# Supporting bonuses
SCORE_DISPLACEMENT      = 10
SCORE_RSI_IDEAL         = 5
SCORE_RSI_PENALTY       = -5
SCORE_RSI_PENALTY_MILD  = -7    # RSI overbought/oversold ringan (70-79 LONG, 21-30 SHORT)
SCORE_RSI_PENALTY_HARD  = -12   # RSI ekstrem (>=80 LONG, <=20 SHORT) — jauh lebih berbahaya
SCORE_EMA_ALIGNED       = 8
SCORE_EMA_BONUS         = 4
SCORE_REF_TF            = 5

# ── SMC Scan Config ──────────────────────────────────────────────────────────
MAX_WORKERS          = 6   # ALIGNED WITH BASE (signal-only) — disamakan 1:1.
# Sebelumnya 1 (sequential murni) untuk hindari burst request ke Binance,
# tapi ini bikin 1 putaran scan 106 pair makan waktu ~6-7 menit (vs ~1 menit
# di signal-only dengan 6 worker paralel) — automatrade jadi sering
# "ketinggalan" momentum setup yang cuma valid sesaat dibanding signal-only.
# Risiko 418 ban di-mitigasi oleh _BINANCE_SEMAPHORE (hanya 1 request
# in-flight ke Binance dalam satu waktu meski workernya 6) + _BINANCE_REQ_DELAY
# + retry/backoff otomatis di fetch_ohlcv_realdata — jadi tetap ada rate
# limiting di level request, cuma pair-nya diproses lebih paralel sekarang.
# PANTAU log untuk "Binance 418" setelah deploy — kalau muncul lagi, turunkan
# nilai ini bertahap (mis. ke 3) sampai stabil.



# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 2 — DATA SOURCE: Binance Futures (direct)
# ═══════════════════════════════════════════════════════════════════════════

_active_data_source = "Binance"

# Konversi interval Binance → string yang diterima endpoint klines
_INTERVAL_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h", "8h": "8h", "12h": "12h",
    "1d": "1d", "3d": "3d", "1w": "1w", "1M": "1M",
}

# OPT: Global rate limiter — batasi max request Binance yang berjalan BERSAMAAN
# Tanpa ini, 3 thread × 5 TF bisa kirim 15 request hampir serentak → 418
#
# FIX (Aug 2026, perf): Semaphore(1) sebelumnya SERI-kan SEMUA request (OHLCV +
# harga + order + akun) jadi satu-satu di seluruh bot, membuat MAX_WORKERS=6
# efeknya PERCUMA — 6 thread tetap antre satu per satu di titik ini. Dengan 86
# pair, ini bikin siklus scan real molor ke ~9-10 menit (target SCAN_INTERVAL
# cuma 60s), karena SIGNALONLY tidak punya semaphore serupa sama sekali dan
# jalan MAX_WORKERS penuh. Dinaikkan ke 4 slot paralel — masih di bawah
# MAX_WORKERS=6 untuk sisakan sedikit headroom, jauh lebih longgar dari 1.
# Binance klines weight cuma 1-5 dari limit 2400/menit, jadi 4 paralel masih
# sangat aman dari 418; _BINANCE_REQ_DELAY tetap jadi jaring pengaman burst.
_BINANCE_SEMAPHORE = threading.Semaphore(4)
# FIX: Naikkan delay dari 0.6 → 1.0 detik antar request agar tidak burst
_BINANCE_REQ_DELAY = 1.5


# ── FIX: OHLCV short-TTL cache (root cause 418 IP ban beruntun) ─────────────
# Root cause 418: BTCUSDT@1h, BTC LTF, dan BTCDOMUSDT di-fetch ULANG dari nol
# di analyze_pair() untuk SETIAP pair × SETIAP mode (bisa 69 pair × 5 mode =
# ratusan request identik per 1 putaran scan), padahal data candle yang sama
# tidak berubah dalam hitungan detik. Ini yang bikin IP kena 418 terus-menerus
# walau sudah ada retry/backoff — retry-nya jalan, tapi sumber masalahnya
# (over-fetching) tidak pernah diperbaiki sampai sekarang.
# Fix: cache hasil fetch per (symbol, interval, limit) selama TTL adaptif
# (lihat _get_cache_ttl). Cukup untuk 1 putaran scan tanpa data basi, tapi
# drastis mengurangi jumlah request ke Binance.
_OHLCV_CACHE: dict = {}
_OHLCV_CACHE_TTL = 20   # TTL flat semua TF (1:1 main73 — lihat _get_cache_ttl)

# ── TTL cache ADAPTIF per timeframe (23 Jul 2026) ───────────────────────────
# Sebelumnya semua TF pakai TTL sama (30 detik) — boros, karena candle 4H/1D
# gak berubah tiap 2 menit tapi tetap di-refetch tiap scan cycle. Dengan 105
# pair sekarang, ini nambah beban request signifikan tanpa manfaat (data HTF
# toh sama aja). TTL sekarang = 25% durasi candle, dibatasi 20 detik (min,
# buat TF kecil kayak 1m/5m tetap fresh) - 900 detik / 15 menit (max, biar
# HTF besar kayak 1D tetap di-refresh berkala, bukan dicache selamanya).
_TF_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800, "12h": 43200,
    "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000,
}

def _get_cache_ttl(tf: str) -> float:
    """TTL cache adaptif per timeframe. HTF besar → cache lebih lama (kurangi
    request), LTF kecil → tetap fresh (data entry harus akurat)."""
    # 1:1 DENGAN main73: main73 TIDAK punya cache OHLCV — tiap scan ambil candle
    # segar. TTL adaptif lama (15m=225s, 1h/4h/1d=900s) bikin candle entry/HTF
    # yang dianalisa bisa basi berkali-kali scan -> sinyal telat/beda dari
    # main73. Sekarang TTL flat 20 detik: cukup hanya untuk dedup request
    # duplikat dalam satu putaran (mis. BTC), tidak pernah basi antar-scan
    # (SCAN_INTERVAL=60s).
    return _OHLCV_CACHE_TTL


def fetch_ohlcv_realdata(symbol_binance: str, interval: str, limit: int = 200) -> pd.DataFrame | None:
    """Ambil OHLCV langsung dari Binance Futures REST API (tanpa ccxt).
    Auto-retry saat kena 418 (IP ban sementara) atau 429 (rate limit).
    Di-cache singkat (_OHLCV_CACHE_TTL detik) untuk hindari fetch duplikat
    beruntun (mis. BTCUSDT@1h yang dipanggil dari banyak tempat per scan).
    """
    tf = _INTERVAL_MAP.get(interval, interval)
    _limit_eff = min(limit, 1500)
    _cache_key = (symbol_binance, tf, _limit_eff)
    _now = time.time()
    _cached = _OHLCV_CACHE.get(_cache_key)
    _ttl_eff = _get_cache_ttl(tf)
    if _cached is not None and (_now - _cached[1]) < _ttl_eff:
        return _cached[0]

    # FIX: selalu mainnet untuk data analisa — lihat komentar MARKET_DATA_BASE_URL
    url = f"{MARKET_DATA_BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol_binance, "interval": tf, "limit": _limit_eff}
    wait_times = [15, 30, 60, 120]  # diperbesar: 418 butuh jeda lebih lama
    retries = 3
    for attempt in range(retries + 1):
        try:
            with _BINANCE_SEMAPHORE:   # max 1 request paralel ke Binance
                r = requests.get(url, params=params, headers=get_headers(), timeout=15)
                time.sleep(_BINANCE_REQ_DELAY)  # jeda kecil antar request, hindari burst

            if r.status_code == 418:
                wait = wait_times[min(attempt, len(wait_times) - 1)]
                print(f"  ⚠️  Binance 418 IP ban sementara {symbol_binance} @ {interval} — tunggu {wait}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(wait)
                    continue
                return None   # habis retry → skip pair ini, jangan crash

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", wait_times[min(attempt, len(wait_times) - 1)]))
                print(f"  ⚠️  Binance 429 rate limit {symbol_binance} @ {interval} — tunggu {retry_after}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(retry_after)
                    continue
                return None

            r.raise_for_status()
            candles = r.json()
            if not candles or len(candles) < 50:
                print(f"  ⚠️  Binance hanya {len(candles) if candles else 0} candles untuk {symbol_binance} @ {interval}")
                return None
            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume",
                "close_time", "qav", "num_trades", "taker_buy_base", "taker_buy_quote", "ignore"
            ])
            df = df[["timestamp", "open", "high", "low", "close", "volume"]].astype(float)
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            _OHLCV_CACHE[_cache_key] = (df, _now)   # cache hanya hasil sukses
            return df
        except requests.HTTPError as e:
            print(f"  ⚠️  Binance HTTP error {symbol_binance} @ {interval}: {e}")
            return None
        except requests.exceptions.Timeout:
            if attempt < retries:
                print(f"  ⚠️  Timeout {symbol_binance} @ {interval} — retry {attempt+1}/{retries}")
                time.sleep(wait_times[min(attempt, len(wait_times) - 1)])
                continue
            print(f"  ❌ Timeout habis {symbol_binance} @ {interval}")
            return None
        except Exception as e:
            print(f"  ❌ Binance fetch gagal {symbol_binance} @ {interval}: {e}")
            return None
    return None


bot_state = {
    "balance_start": 0,
    "wins": 0,
    "losses": 0,
    "win_streak": 0,
    "lose_streak": 0,
    "signals_today": 0,
    "last_summary_date": None,
    "start_time": None,
    # ── PNL tracking harian & bulanan ──────────────────────────────────────
    "balance_day_start": 0,
    "balance_day_date": None,
    "balance_month_start": 0,
    "balance_month_key": None,
    # ── Win/Loss harian & bulanan ──────────────────────────────────────────
    "day_wins": 0,
    "day_losses": 0,
    "month_wins": 0,
    "month_losses": 0,
    # ── PNL kumulatif terpisah per mode (LIVE / DEMO) ──────────────────────
    "mode_pnl": {
        "LIVE": {"realized": 0.0, "start_bal": 0.0, "wins": 0, "losses": 0},
        "DEMO": {"realized": 0.0, "start_bal": 0.0, "wins": 0, "losses": 0},
    },
}

active_positions = {}
_positions_lock = threading.Lock()   # FIX BUG #3: proteksi active_positions dari concurrent access
_perf_lock      = threading.Lock()   # FIX D: proteksi bot_state wins/losses dari concurrent update

# ── FIX BUG: duplicate-close guard ───────────────────────────────────────────
# ROOT CAUSE (ditemukan 2026-08-31 dari analisa trade_log): manage_trailing()
# bisa salah-deteksi "posisi closed" untuk symbol yang SEBENARNYA masih open
# di Binance (has_position() sempat return False keliru — race/stale read),
# lalu record_trade_close() + notify_sl_hit() terlanjur jalan dan symbol
# dihapus dari active_positions. Loop berikutnya melihat symbol ini "baru"
# lagi (entry/lot yang tersimpan di posisi asli kebetulan masih identik)
# dan mengulang deteksi-close yang sama berkali-kali dalam hitungan menit.
# Kasus nyata: NEARUSDT tercatat "SL" 18× dalam 33 menit dengan entry & lot
# yang PERSIS SAMA di setiap baris — mustahil untuk 18 posisi independen,
# jadi ini satu posisi yang di-log berulang, bukan 18 kekalahan beruntun.
#
# Guard ini menyimpan {symbol: last_close_ts} setelah sebuah close benar-benar
# di-commit (record_trade_close dipanggil). Kalau symbol yang sama "muncul
# lagi" untuk diproses closed dalam DUPLICATE_CLOSE_GUARD_SECONDS berikutnya,
# manage_trailing() WAJIB verifikasi ulang ke Binance (bukan langsung percaya
# has_position()==False sekali baca) sebelum boleh commit close kedua.
_recent_close_guard: dict = {}          # symbol -> unix timestamp closed terakhir
DUPLICATE_CLOSE_GUARD_SECONDS = 180     # 3 menit — cukup untuk menutup celah race trailing loop (poll tiap 8 detik)
# FIX RACE TP1-HEAL vs CLEANUP-DUPLICATE: manage_trailing() (thread trailing,
# poll tiap TRAILING_POLL_INTERVAL detik) dan cleanup_duplicate_position_orders()
# (dipanggil dari main loop tiap AUTO_CLEANUP_INTERVAL_SECONDS) sama-sama
# create/cancel order SL/TP conditional di Binance untuk symbol yang sama,
# tanpa saling tahu. Kalau overlap: trailing thread lagi pasang ulang TP1
# (setelah deteksi CANCELED) di saat yang sama cleanup lagi menghapus order
# "duplikat" — race ini bisa looping (pasang ulang → salah dianggap
# duplikat → dihapus → trailing thread deteksi CANCELED lagi → pasang ulang
# lagi ...), menghasilkan puluhan order dalam waktu singkat.
# Lock ini dipegang oleh KEDUA proses saat create/cancel order conditional
# per symbol, supaya keduanya tidak pernah jalan bersamaan untuk symbol sama.
_order_mgmt_lock = threading.Lock()

# ── Pending Limit Orders ─────────────────────────────────────────────────────
# Menyimpan limit order yang sudah dikirim tapi belum terisi (belum jadi posisi).
# Format: {symbol: {"order_id": int, "direction": str, "entry": float, ...}}
pending_limit_orders: dict = {}

# Timeout limit order: SAFETY NET saja, bukan trigger utama.
# Trigger utama untuk cancel adalah precheck TP1-touch di monitor_pending_limit_orders()
# (kalau harga sudah sampai TP1 sebelum entry ke-fill → setup dianggap invalid → cancel).
# Order LIMIT sengaja dibiarkan pending selama TP1 belum kesentuh dan entry belum fill —
# gak lagi di-cancel cuma karena waktu habis padahal setup masih valid (kasus GMXUSDT
# 27 Jul 2026: expired di 500 menit padahal TP1 belum kesentuh sama sekali).
# Angka di bawah cuma jaring pengaman terakhir (bug lain / API stuck / posisi zombie),
# BUKAN target waktu tunggu normal.
LIMIT_ORDER_TIMEOUT_MINUTES: int = 360    # v79: 6 jam (12 candle 30m) — limit lama fill saat setup sudah basi

DAILY_SUMMARY_HOUR_UTC = 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 3 — TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

# Offset polling Telegram (untuk command listener)
_tg_offset = 0


def send_telegram_raw(msg):
    """
    Kirim pesan ke Telegram dengan error handling lengkap:
    - Auto-split jika pesan > 4096 karakter (batas Telegram)
    - Retry otomatis jika rate-limited (429)
    - Retry otomatis (3x, backoff naik) kalau network error murni (timeout/
      connection reset/dsb) — sebelumnya kasus ini langsung nyerah diam-diam
      dan pesan (misal notif TP1 HIT) bisa hilang tanpa jejak selain log console.
    - Fallback ke plain text jika HTML parse_mode gagal
    - Log detail error ke console agar tidak hilang diam-diam
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    MAX_LEN = 4000  # sedikit di bawah 4096 untuk safety margin

    def _send(text, parse_mode="HTML"):
        _NET_RETRY_DELAYS = (1, 3, 6)   # detik, backoff naik
        for _i, _delay in enumerate((0, *_NET_RETRY_DELAYS)):
            if _delay:
                time.sleep(_delay)
            try:
                r = requests.post(url, data={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       text,
                    "parse_mode": parse_mode,
                }, timeout=10)
                return r
            except Exception as e:
                _left = len(_NET_RETRY_DELAYS) - _i
                print(f"⚠️ Telegram network error: {e}"
                      + (f" — retry ({_left} attempt lagi)" if _left > 0 else " — menyerah, pesan hilang"))
        return None

    def _send_with_fallback(text):
        """Kirim satu chunk — coba HTML dulu, fallback plain text kalau gagal."""
        r = _send(text, parse_mode="HTML")
        if r is None:
            return
        if r.ok:
            return
        # Rate limit
        if r.status_code == 429:
            try:
                retry_after = r.json().get("parameters", {}).get("retry_after", 5)
            except Exception:
                retry_after = 5
            print(f"⚠️ Telegram rate limit — tunggu {retry_after}s lalu retry")
            time.sleep(retry_after)
            r = _send(text, parse_mode="HTML")
            if r is None or r.ok:
                return
        # HTML parse error atau message too long → fallback plain text
        if not r.ok:
            try:
                err_desc = r.json().get("description", "")
            except Exception:
                err_desc = r.text[:200]
            print(f"⚠️ Telegram HTML parse error — fallback ke plain text. Desc: {err_desc}")
            import re
            plain = re.sub(r"<[^>]+>", "", text)
            r2 = _send(plain, parse_mode="")
            if r2 is None or not r2.ok:
                print(f"⚠️ Telegram plain text fallback juga gagal: {r2.status_code if r2 else 'N/A'}")

    # ── Auto-split jika pesan terlalu panjang ─────────────────────────────────
    if len(msg) <= MAX_LEN:
        _send_with_fallback(msg)
    else:
        # Split per baris agar tidak memotong di tengah kata/HTML tag
        lines  = msg.split("\n")
        chunk  = ""
        part   = 1
        for line in lines:
            candidate = (chunk + "\n" + line) if chunk else line
            if len(candidate) > MAX_LEN:
                # Kirim chunk sekarang, mulai chunk baru
                header = f"[{part}/…]\n" if part == 1 else f"[lanjutan {part}]\n"
                _send_with_fallback(header + chunk)
                time.sleep(0.5)   # jeda kecil antar part biar tidak kena rate limit
                chunk = line
                part += 1
            else:
                chunk = candidate
        if chunk:
            suffix = f" [{part}]" if part > 1 else ""
            _send_with_fallback(chunk + (f"\n<i>— end{suffix} —</i>" if part > 1 else ""))


def send_telegram_document(file_bytes: bytes, filename: str, caption: str = "", chat_id: str = None) -> bool:
    """
    Kirim file (misal CSV trade log) ke Telegram sebagai dokumen yang bisa didownload.
    file_bytes : isi file dalam bentuk bytes (bukan path — supaya tidak perlu tulis ke disk permanen)
    filename   : nama file yang muncul di Telegram, contoh 'trade_log_30d.csv'
    caption    : teks singkat yang menyertai file
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    try:
        r = requests.post(
            url,
            data={"chat_id": chat_id or TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "HTML"},
            files={"document": (filename, file_bytes)},
            timeout=30,
        )
        if r.ok:
            return True
        print(f"⚠️ Telegram sendDocument gagal: {r.status_code} {r.text[:200]}")
        return False
    except Exception as e:
        print(f"⚠️ Telegram sendDocument network error: {e}")
        return False


# Coin yang harganya sangat kecil → butuh 8 desimal
_HIGH_DECIMAL_PAIRS = {"1000PEPE/USDT", "1000SHIB/USDT"}


def fmt_price(pair: str, price: float) -> str:
    """Format harga dengan presisi yang sesuai per coin.
    - 1000PEPE/USDT, 1000SHIB/USDT → 8 desimal
    - Semua pair lain              → 4 desimal
    """
    if pair in _HIGH_DECIMAL_PAIRS:
        return f"{price:.8f}"
    return f"{price:.4f}"


def send_telegram_signal(signal: dict, mode: dict, score_bd: dict, session: str,
                         pa_name: str, btc_bias: str, btcd_trend: str, macro_reason: str,
                         allow_rr_below_floor: bool = False):
    """
    allow_rr_below_floor: default False (perilaku pra-eksekusi — sinyal BARU
    yang RR aktualnya di bawah RR_GRADE_B setelah rounding/tick-size TIDAK
    dikirim, lihat guard RR floor di bawah).

    Set True HANYA saat dipanggil dari _activate_position_from_limit() (limit
    order SUDAH terisi, posisi SUDAH live di exchange dengan SL/TP terpasang).
    ROOT CAUSE bug "card sinyal hilang pas limit terisi" (kasus nyata:
    DOGEUSDT — limit terisi, notif ringkas "LIMIT ORDER TERISI" muncul, tapi
    signal card lengkap TIDAK PERNAH terkirim): fungsi ini dipakai ulang utk
    dua konteks berbeda (sinyal baru vs notif pasca-fill), tapi guard RR floor
    di bawah nge-`return` tanpa pandang konteks — di titik pasca-fill, posisi
    SUDAH berjalan; membatalkan card tidak membatalkan trade, cuma
    menyembunyikan info dari user. Sekarang guard itu HANYA mem-block
    pengiriman kalau allow_rr_below_floor=False (sinyal pra-eksekusi); untuk
    notif pasca-fill, card TETAP dikirim, cukup diberi label peringatan RR.
    """
    pair     = signal["pair"]
    dir_str  = signal["direction"]
    dir_em   = "🟢" if dir_str == "LONG" else "🔴"
    grade    = signal.get("grade", "B")
    grade_em = "🏆" if grade == "A" else "🥈"
    tps      = signal["take_profit"]

    bar_filled = min(10, int(signal["score"] / 10))
    bar = "█" * bar_filled + "░" * (10 - bar_filled)

    try:
        # PORTED DARI main20.py — score_bd sekarang berskema SMC flow murni
        # (htf/bos/momentum/liquidity/entry_zone), bukan lagi skema
        # price_action/supply_demand/support_resistance dari sistem lama.
        def _signed(v):
            return f"{'+' if v >= 0 else ''}{v}"
        breakdown_lines = "\n".join([
            f"  HTF Bias       : {_signed(score_bd.get('htf', 0))} pts",
            f"  BOS/Structure  : {_signed(score_bd.get('bos', 0))} pts",
            f"  Momentum       : {_signed(score_bd.get('momentum', 0))} pts",
            f"  Liquidity Sweep: {_signed(score_bd.get('liquidity', 0))} pts",
            f"  Entry Zone     : {_signed(score_bd.get('entry_zone', 0))} pts  (OB/FVG)",
            f"  Displacement   : {_signed(score_bd.get('displacement', 0))} pts",
            f"  Volume         : {_signed(score_bd.get('volume', 0))} pts",
            f"  Session        : {_signed(score_bd.get('session', 0))} pts",
            f"  RSI            : {_signed(score_bd.get('rsi', 0))} pts",
            f"  EMA            : {_signed(score_bd.get('ema', 0))} pts",
            f"  Ref TF         : {_signed(score_bd.get('ref_tf', 0))} pts",
            f"  Macro          : {_signed(score_bd.get('macro', 0))} pts",
        ])
    except Exception as bd_err:
        print(f"⚠️ send_telegram_signal: gagal build breakdown: {bd_err}")
        breakdown_lines = "  (detail tidak tersedia)"

    try:
        def _esc(s: str) -> str:
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        reasons_str = "\n".join([f"  • {_esc(r)}" for r in signal.get("reason", [])])
    except Exception:
        reasons_str = "  (reasons tidak tersedia)"

    # ── Technical Snapshot (RSI / MACD / Resistance-Support terdekat) ───────
    # Display-only, dihitung di analyze_pair() (SECTION 10b) — tidak
    # mempengaruhi score/gate sama sekali, murni tambahan conviction visual
    # di card, mirip alat riset seperti Neurobro.
    tech_snapshot_line = ""
    try:
        _rsi_val = signal.get("_rsi_entry")
        _macd    = signal.get("_macd") or {}
        _lvls    = signal.get("_nearest_levels") or {}

        if _rsi_val is not None or _macd or _lvls:
            _rsi_txt = f"{_rsi_val:.1f}" if _rsi_val is not None else "-"

            _macd_trend = _macd.get("trend", "-")
            _macd_em    = "🟢" if _macd_trend == "BULLISH" else ("🔴" if _macd_trend == "BEARISH" else "⚪")
            _macd_txt   = f"{_macd_em} {_macd_trend} (hist {_macd.get('histogram', 0):+.5f})"
            if _macd.get("crossover"):
                _cross_txt = "Bullish Cross ↑" if _macd["crossover"] == "BULLISH_CROSS" else "Bearish Cross ↓"
                _macd_txt += f" — {_cross_txt}"

            _res = _lvls.get("resistance")
            _sup = _lvls.get("support")
            _res_txt = f"${fmt_price(pair, _res)}" if _res is not None else "-"
            _sup_txt = f"${fmt_price(pair, _sup)}" if _sup is not None else "-"

            tech_snapshot_line = (
                f"📈 <b>Technical Snapshot</b>\n"
                f"  RSI            : {_rsi_txt}\n"
                f"  MACD           : {_macd_txt}\n"
                f"  Resistance dkt : {_res_txt}\n"
                f"  Support dkt    : {_sup_txt}\n"
                f"{'─'*38}\n"
            )
    except Exception as ts_err:
        print(f"⚠️ send_telegram_signal: gagal build technical snapshot: {ts_err}")
        tech_snapshot_line = ""

    _rr_warning_line = ""
    try:
        _entry_val = signal['entry']
        _sl_val    = signal['stop_loss']
        # TP1/TP2 (partial): tps[0] = TP1 (RR_GRADE_B), tps[-1] = TP2 (RR_GRADE_A+0.5).
        _tp1_target = tps[0]  if tps else 0.0
        _tp2_target = tps[-1] if tps else 0.0
        _sl_pct     = abs(_entry_val - _sl_val) / max(_entry_val, 1e-9) * 100
        _tp1_pct    = abs(_tp1_target - _entry_val) / max(_entry_val, 1e-9) * 100
        _tp2_pct    = abs(_tp2_target - _entry_val) / max(_entry_val, 1e-9) * 100
        # FIX: RR yang ditampilkan HARUS dihitung dari _sl_pct/_tpN_pct yang
        # sama persis dengan yang dipakai di baris Stop Loss & Target TP di
        # bawah — sebelumnya pakai signal['RR']/signal['rr'] yang di-set saat
        # sinyal PERTAMA dibuat, jadi bisa basi/nggak sinkron kalau TP/SL
        # berubah setelahnya (tick-size rounding, clamp mode, dst).
        # Kasus nyata: ARUSDT tampil "1:1.98" padahal SL -1.03% & TP +0.76%
        # (RR real cuma ~0.74) — sinyal itu SEHARUSNYA gagal RR gate.
        _rr1_display = round(_tp1_pct / _sl_pct, 2) if _sl_pct > 0 else 0.0
        _rr2_display = round(_tp2_pct / _sl_pct, 2) if _sl_pct > 0 else 0.0
        _rr_display  = _rr2_display  # dipakai guard floor di bawah (RR TP2, target akhir)

        # ── GUARD RR FLOOR (pre-send, HANYA untuk sinyal BARU) ──────────────
        # rr1/grade yang lolos di gate analyze_pair() (grade_signal) DIHITUNG
        # dari calculate_rr() — cek ulang di sini pakai angka _rr1/_rr2_display
        # yang SAMA dengan yang ditampilkan ke user (setelah tick-size
        # rounding & fmt_price) — kalau TP1 sudah di bawah minimum, jangan
        # kirim (untuk sinyal baru). Untuk notif pasca-fill
        # (allow_rr_below_floor=True), posisi SUDAH live, card TETAP dikirim
        # + label peringatan.
        if _rr1_display < RR_GRADE_B:
            if not allow_rr_below_floor:
                print(f"  ⛔ [send_telegram_signal] {pair} — RR1 aktual {_rr1_display} < {RR_GRADE_B} "
                      f"setelah rounding/tick-size (grade gate awal pakai RR clamp, jadi lolos) — sinyal DIBATALKAN")
                return
            else:
                print(f"  ⚠️  [send_telegram_signal] {pair} — RR1 aktual {_rr1_display} < {RR_GRADE_B} "
                      f"tapi posisi SUDAH live (allow_rr_below_floor=True) — card tetap dikirim + warning")
                _rr_warning_line = (
                    f"⚠️ <b>RR1 aktual {_rr1_display} di bawah minimum {RR_GRADE_B}</b> "
                    f"(bergeser setelah tick-size/rounding)\n"
                )


        tp1_pct_lot = int(TP1_PARTIAL * 100)
        tp2_pct_lot = 100 - tp1_pct_lot
        if tp2_pct_lot > 0:
            _tp_lines = (
                f"🎯 TP1      : ${fmt_price(pair, _tp1_target)}  (+{_tp1_pct:.2f}%)  (1:{_rr1_display})  — {tp1_pct_lot}% lot\n"
                f"🎯 TP2      : ${fmt_price(pair, _tp2_target)}  (+{_tp2_pct:.2f}%)  (1:{_rr2_display})  — {tp2_pct_lot}% lot (sisa, trailing BE)\n"
            )
        else:
            _tp_lines = (
                f"🎯 Target TP : ${fmt_price(pair, _tp1_target)}  (+{_tp1_pct:.2f}%)  (1:{_rr1_display})\n"
            )

        msg = (
            f"{dir_em} <b>{pair} — {dir_str}</b>  {grade_em} Grade {grade}\n"
            f"{'─'*38}\n"
            f"{_rr_warning_line}"
            f"📊 Mode     : {mode['label']} ({mode['htf_tf']} → {mode['entry_tf']})\n"
            f"🕐 Session  : {session}\n"
            f"{'─'*38}\n"
            f"💰 Entry    : <b>${fmt_price(pair, _entry_val)}</b>\n"
            f"🛑 Stop Loss: ${fmt_price(pair, _sl_val)}  <b>(-{_sl_pct:.2f}% dari entry)</b>\n"
            f"{_tp_lines}"
            f"{'─'*38}\n"
            f"🧮 Score    : <b>{signal['score']} pts</b>\n"
            f"  [{bar}]\n"
            f"{breakdown_lines}\n"
            f"{'─'*38}\n"
            f"{tech_snapshot_line}"
            + (
                f"{'─'*38}\n"
                f"🧲 Zone Confluence ({score_bd.get('zone_confluence', 0):+d} pts):\n"
                + "\n".join([f"  • {_esc(r)}" for r in signal.get('zone_confluence_reasons', [])]) + "\n"
                if signal.get('zone_confluence_reasons') else ""
            ) +
            f"{'─'*38}\n"
            f"📋 Reasons:\n{reasons_str}\n"
            f"{'─'*38}\n"
            f"🪙 BTC ({BTC_BIAS_TF}): {btc_bias} | BTC.D: {btcd_trend}\n"
            f"  {_esc(macro_reason)}\n"
            f"{'─'*38}\n"
            f"⚡ Leverage: <b>{signal.get('_leverage', '?')}x</b> | Mode: <b>{'🔴 LIVE' if BOT_MODE == 'LIVE' else '🟢 DEMO'}</b>"
        )
        send_telegram_raw(msg)
    except Exception as e:
        print(f"⚠️ send_telegram_signal gagal build pesan penuh: {e} — kirim notif minimal")
        try:
            send_telegram_raw(
                f"{dir_em} <b>ENTRY {pair} — {dir_str}</b>\n"
                f"Entry: {signal.get('entry','?')} | SL: {signal.get('stop_loss','?')}\n"
                f"TP1: {tps[0] if tps else '?'} | TP2: {tps[-1] if tps else '?'}\n"
                f"Score: {signal.get('score','?')} pts | Grade {grade}\n"
                f"⚠️ Detail sinyal tidak bisa ditampilkan (error: {e})"
            )
        except Exception as fallback_err:
            print(f"❌ Notif minimal juga gagal: {fallback_err}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 4 — BINANCE API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

# ── Server Time Sync (FIX v35) ───────────────────────────────────────────────
# Masalah: jam lokal server bisa drift jauh dari jam Binance → timestamp
# di-reject dengan 400 Bad Request, walaupun recvWindow sudah besar.
# Solusi: ambil serverTime dari Binance sekali, hitung offset, lalu pakai
# offset itu di setiap sign_request agar timestamp selalu valid.
# Cache di-refresh setiap 5 menit agar tidak spam /fapi/v1/time.
_server_time_offset_ms: int = 0          # selisih (serverTime - localTime) dalam ms
_server_time_last_sync: float = 0.0      # epoch detik saat terakhir sync
_SERVER_TIME_SYNC_INTERVAL = 300         # sync ulang tiap 5 menit
_server_time_lock = threading.Lock()

def _sync_server_time(force: bool = False):
    """
    Ambil serverTime dari Binance untuk koreksi jam lokal yang drift.
    Di-cache 5 menit. Thread-safe.
    Dipanggil otomatis oleh sign_request() — tidak perlu panggil manual.

    FIX v38b: Selalu pakai MAINNET untuk sync waktu, bukan testnet.
    FIX v39b: Parameter force=True untuk skip cache — wajib dipakai sebelum
    order kritis (SL, emergency close, TP) agar tidak 400 karena offset stale.
    """
    global _server_time_offset_ms, _server_time_last_sync
    now_local = time.time()
    with _server_time_lock:
        if not force and now_local - _server_time_last_sync < _SERVER_TIME_SYNC_INTERVAL:
            return  # masih fresh, tidak perlu sync

        # Coba endpoint secara berurutan — berhenti di yang pertama berhasil
        _time_endpoints = [
            "https://fapi.binance.com/fapi/v1/time",      # Futures mainnet — paling akurat
            "https://api.binance.com/api/v3/time",         # Spot mainnet — fallback
            "https://api1.binance.com/api/v3/time",        # Mirror spot — fallback kedua
        ]
        for _ep in _time_endpoints:
            try:
                _r = requests.get(_ep, timeout=5)
                _r.raise_for_status()
                _srv_time = _r.json().get("serverTime", 0)
                if _srv_time:
                    _local_ms = int(time.time() * 1000)
                    _new_offset = _srv_time - _local_ms
                    _prev_offset = _server_time_offset_ms
                    _server_time_offset_ms = _new_offset
                    _server_time_last_sync = time.time()
                    if abs(_new_offset) > 500:
                        print(
                            f"  ⏱️  Server time sync ({_ep.split('/')[2]}): "
                            f"offset={_new_offset:+d}ms "
                            f"({'jam lokal maju' if _new_offset < 0 else 'jam lokal mundur'}) "
                            f"[sebelumnya {_prev_offset:+d}ms]"
                        )
                    return  # berhasil, tidak perlu coba endpoint lain
            except Exception as _ste:
                print(f"  ⚠️  Server time sync gagal ({_ep.split('/')[2]}): {_ste}")

        # Semua endpoint gagal — offset lama tetap dipakai
        print(f"  ❌ Server time sync gagal semua endpoint — pakai offset lama ({_server_time_offset_ms:+d}ms)")

def sign_request(params):
    # ── FIX v35: pakai timestamp yang sudah dikoreksi offset server Binance ──
    # Ini mencegah 400 Bad Request akibat jam lokal drift dari jam Binance.
    # FIX v40: recvWindow Binance Futures MAKSIMAL 60000ms. Nilai 90000ms
    # sebelumnya SELALU ditolak 400 Bad Request oleh Binance (ini akar
    # masalah "gagal fetch openOrders: 400 Client Error" di log) —
    # bukan masalah jaringan/testnet flaky seperti yang dikira sebelumnya.
    _sync_server_time()
    params = dict(params)
    params["timestamp"]  = int(time.time() * 1000) + _server_time_offset_ms
    params["recvWindow"] = 59000   # aman di bawah limit resmi 60000ms
    query = urlencode(params)
    _, api_secret = get_api_credentials()
    sig   = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    return params

def get_headers():
    api_key, _ = get_api_credentials()
    return {"X-MBX-APIKEY": api_key}


def verify_api_connectivity() -> tuple:
    """
    Test koneksi ke Binance SEBELUM bot mulai trading beneran.
    Cek: API key valid, IP sudah di-whitelist (kalau IP restriction aktif di
    key-nya), dan permission Futures aktif. Dipanggil di /start — kalau gagal,
    /start DITOLAK (bot tetap paused) supaya tidak lanjut order dan malah
    kena error -2015 di tengah jalan tanpa notifikasi jelas ke user.

    Returns: (ok: bool, message: str)
    """
    try:
        url     = get_base_url() + "/fapi/v2/account"
        params  = sign_request({})
        headers = get_headers()
        r = requests.get(url, params=params, headers=headers, timeout=15)

        if r.ok:
            return True, "Koneksi & API key OK."

        try:
            err  = r.json()
            code = err.get("code")
            msg  = err.get("msg", "")
        except Exception:
            code, msg = None, r.text[:200]

        if code == -2015:
            return False, (
                "API-key, IP, atau permission tidak valid (-2015). Kemungkinan:\n"
                "  • IP server ini belum di-whitelist di Binance API Management\n"
                "  • API key salah / sudah di-revoke\n"
                "  • Permission \"Enable Futures\" belum di-centang di API key ini"
            )
        if code == -2014:
            return False, "Format API key tidak valid (-2014). Cek API_KEY/API_SECRET di config."
        if code == -1021:
            return False, "Timestamp tidak sinkron dengan server Binance (-1021). Coba /start lagi."
        if code == -2008:
            return False, "API key tidak valid / tidak ditemukan (-2008)."
        return False, f"Binance menolak koneksi (HTTP {r.status_code}, code={code}, msg={msg})"

    except requests.exceptions.Timeout:
        return False, "Timeout menghubungi Binance — cek koneksi internet server."
    except requests.exceptions.RequestException as e:
        return False, f"Gagal konek ke Binance: {e}"
    except Exception as e:
        return False, f"Error tak terduga saat cek koneksi: {e}"

def _api_request(method: str, path: str, params: dict, retries: int = 4, signed: bool = False) -> dict:
    """
    Wrapper request dengan auto-retry untuk rate limit Binance:
    - 418 = IP banned sementara (terlalu banyak request) → tunggu lama
    - 429 = rate limit → tunggu sesuai Retry-After header
    Exponential backoff: 15s, 30s, 60s, 120s
    FIX v39f: sign_request dipanggil DI DALAM semaphore agar timestamp
    di-generate tepat saat request mau dikirim — bukan sebelum tunggu semaphore.
    Ini mencegah 400 Bad Request saat semaphore locked lama oleh fetch_ohlcv.
    """
    url     = get_base_url() + path
    headers = get_headers()
    wait_times = [15, 30, 60, 120]  # diperbesar: 418 butuh jeda lebih lama

    for attempt in range(retries + 1):
        try:
            with _BINANCE_SEMAPHORE:   # max 1 request paralel ke Binance
                # FIX v39f: generate timestamp SETELAH acquire semaphore
                # agar tidak stale kalau tadi nunggu semaphore lama
                _signed_params = sign_request(dict(params)) if signed else params
                if method == "GET":
                    r = requests.get(url, params=_signed_params, headers=headers, timeout=15)
                elif method == "POST":
                    r = requests.post(url, params=_signed_params, headers=headers, timeout=15)
                elif method == "DELETE":
                    r = requests.delete(url, params=_signed_params, headers=headers, timeout=15)
                else:
                    raise ValueError(f"Unknown method: {method}")
                time.sleep(_BINANCE_REQ_DELAY)   # jeda setelah setiap request

            if r.status_code == 418:
                wait = wait_times[min(attempt, len(wait_times) - 1)]
                print(f"  ⚠️  Binance 418 IP ban sementara — tunggu {wait}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(wait)
                    continue
                r.raise_for_status()

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", wait_times[min(attempt, len(wait_times) - 1)]))
                print(f"  ⚠️  Binance 429 rate limit — tunggu {retry_after}s (attempt {attempt+1}/{retries+1})")
                if attempt < retries:
                    time.sleep(retry_after)
                    continue
                r.raise_for_status()

            if r.status_code == 408:
                # 408 dari testnet.binancefuture.com = request timeout di sisi
                # SERVER Binance (testnet memang sering flaky), bukan error
                # dari client. Sebelumnya kode ini langsung jatuh ke
                # raise_for_status() tanpa retry -> itu sebabnya cleanup gagal
                # instan. Sekarang di-retry sama seperti Timeout biasa.
                wait = min(wait_times[min(attempt, len(wait_times) - 1)], 10)
                print(f"  ⚠️  Binance 408 request timeout (testnet flaky) — retry {attempt+1}/{retries+1} in {wait}s")
                if attempt < retries:
                    time.sleep(wait)
                    continue
                r.raise_for_status()

            if not r.ok and method == "POST":
                try:
                    err = r.json()
                    print(f"  ❌ api_{method.lower()} {path} HTTP {r.status_code} | code={err.get('code')} msg={err.get('msg')}")
                except Exception:
                    print(f"  ❌ api_{method.lower()} {path} HTTP {r.status_code} | body={r.text[:300]}")

            r.raise_for_status()
            return r.json()

        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries:
                print(f"  ⚠️  Timeout {path} — retry {attempt+1}/{retries}")
                time.sleep(wait_times[min(attempt, len(wait_times) - 1)])
                continue
            raise
        except Exception:
            raise

    raise RuntimeError(f"API {method} {path} gagal setelah {retries+1} attempt")


def api_get(path, params=None, signed=False):
    if params is None:
        params = {}
    # FIX v39f: pass signed flag to _api_request — sign inside semaphore
    return _api_request("GET", path, params, signed=signed)

def api_post(path, params=None):
    if params is None:
        params = {}
    # FIX v39f: sign_request dipanggil di dalam _api_request (setelah acquire semaphore)
    # agar timestamp di-generate SAAT request mau dikirim, bukan sebelum tunggu semaphore
    return _api_request("POST", path, params, signed=True)

def api_delete(path, params=None):
    if params is None:
        params = {}
    return _api_request("DELETE", path, params, signed=True)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 5 — BALANCE & POSITION
# ═══════════════════════════════════════════════════════════════════════════

def init_balance():
    # FIX v42: sebelumnya tanpa try/except sama sekali — kalau Binance API
    # gagal total (network issue, endpoint down/migrasi, dll) exception ini
    # menjalar sampai crash bot & Railway auto-restart container. Ini bikin
    # CRASH-LOOP tak berujung: startup 30s countdown → gagal fetch balance →
    # container mati → Railway restart → ulang dari awal, terus-menerus.
    # Sekarang: kalau gagal, bot LANJUT jalan (standby, tidak fatal) — balance
    # akan dicoba lagi otomatis lewat get_balance() saat dibutuhkan nanti.
    try:
        data = api_get("/fapi/v2/balance", signed=True)
    except Exception as e:
        print(f"⚠️  init_balance gagal fetch balance (bot tetap lanjut jalan): {e}")
        send_telegram_raw(
            f"⚠️ <b>Gagal ambil balance awal saat startup</b>\n"
            f"Error: {e}\n"
            f"Bot tetap jalan — balance akan dicoba ulang otomatis."
        )
        return 0
    for asset in data:
        if asset["asset"] == "USDT":
            bal = float(asset["balance"])
            # Hanya set balance_start jika belum ada (jangan timpa nilai dari load_state)
            # Ini menjaga PnL kumulatif tetap valid setelah restart
            if bot_state["balance_start"] == 0:
                bot_state["balance_start"] = bal
            bot_state["start_time"] = datetime.now(timezone.utc)
            # Set snapshot harian & bulanan saat pertama start
            refresh_pnl_snapshots(bal)
            # Set start_bal per mode jika belum pernah diset
            if bot_state["mode_pnl"][BOT_MODE]["start_bal"] == 0:
                bot_state["mode_pnl"][BOT_MODE]["start_bal"] = bal
            # ── Init daily limit snapshot ─────────────────────────────────────
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if _daily_limit_state["date"] != today:
                _daily_limit_state["date"]         = today
                _daily_limit_state["balance_open"] = bal
                _daily_limit_state["paused_by"]    = None
                _daily_limit_state["auto_started"] = False
                print(f"📅 Daily limit init — {today} | Balance open: {bal:.2f} USDT")
            print(f"💰 Balance BOT: {bal} USDT")
            send_telegram_raw(
                f"🚀 <b>Bot SMC Auto Trade Started</b>\n"
                f"💰 Balance : <b>{bal} USDT</b>\n"
                f"🌐 Mode    : <b>{'🔴 LIVE (mainnet)' if BOT_MODE == 'LIVE' else '🟢 DEMO (testnet)'}</b>\n"
                f"ℹ️ Gunakan /changeliveordemo untuk ganti mode."
            )
            return bal
    return 0

def get_balance():
    """Ambil available balance USDT. Retry 3x jika 0 (kadang timing issue setelah posisi open).
    FIX: return None (bukan 0) jika API error — agar caller bisa bedakan 'error' vs 'saldo memang 0'.
    """
    for _attempt in range(3):
        try:
            data = api_get("/fapi/v2/balance", signed=True)
            for asset in data:
                if asset["asset"] == "USDT":
                    val = float(asset["availableBalance"])
                    if val > 0 or _attempt == 2:
                        return val
            if _attempt < 2:
                time.sleep(0.5)
        except Exception as e:
            if _attempt == 2:
                print(f"  ⚠️  get_balance error: {e} — return None (bukan 0) agar tidak salah hitung PnL")
                return None   # FIX: return None agar caller tahu ini error, bukan saldo 0
            time.sleep(0.5)
    return None   # FIX: return None setelah semua retry habis

def get_total_balance():
    """Ambil total balance USDT (termasuk margin terpakai). Retry 3x jika 0.
    FIX: return None (bukan 0) jika API error — agar caller tidak salah hitung daily PnL sebagai -100%.
    Kalau API error saat IP ban dan return 0, pnl_pct = (0 - balance_open)/balance_open = -100% → bot pause!
    """
    for _attempt in range(3):
        try:
            data = api_get("/fapi/v2/balance", signed=True)
            for asset in data:
                if asset["asset"] == "USDT":
                    val = float(asset["balance"])
                    if val > 0 or _attempt == 2:
                        return val
            if _attempt < 2:
                time.sleep(0.5)
        except Exception as e:
            if _attempt == 2:
                print(f"  ⚠️  get_total_balance error: {e} — return None agar tidak trigger daily loss -100%")
                return None   # FIX: jangan return 0! Caller HARUS handle None dengan skip cek
            time.sleep(0.5)
    return None   # FIX: return None setelah semua retry habis

def _mark_position_closed(symbol: str):
    """Catat waktu commit-close untuk symbol ini — dipakai duplicate-close guard."""
    _recent_close_guard[symbol] = time.time()


def _is_recently_closed(symbol: str) -> bool:
    """True kalau symbol ini baru saja di-commit closed dalam window guard."""
    ts = _recent_close_guard.get(symbol)
    if ts is None:
        return False
    return (time.time() - ts) < DUPLICATE_CLOSE_GUARD_SECONDS


def confirm_position_really_closed(symbol: str, retries: int = 2, delay_seconds: float = 1.5) -> bool:
    """
    Verifikasi ULANG ke Binance apakah posisi symbol ini benar-benar closed —
    dipanggil sebelum commit close kedua untuk symbol yang baru saja closed
    (lihat _is_recently_closed). has_position() sekali baca bisa keliru
    karena race/stale response; retry singkat dengan jeda memberi waktu
    Binance konsisten, dan HANYA kalau SEMUA percobaan sepakat "tidak ada
    posisi" baru dianggap benar-benar closed.
    """
    for i in range(retries):
        if has_position(symbol):
            print(f"  🛡️  [{symbol}] Duplicate-close guard: posisi TERNYATA masih open "
                  f"(percobaan verifikasi #{i+1}/{retries}) — batalkan proses close kedua.")
            return False
        if i < retries - 1:
            time.sleep(delay_seconds)
    return True


def has_position(symbol: str) -> bool:
    """
    Cek apakah posisi BENAR-BENAR masih open di Binance (query langsung).
    Tidak mengandalkan active_positions memory saja — bisa stale kalau
    posisi close dari luar (TP/SL kena, atau manual close di app Binance).
    Juga cek pending_limit_orders agar tidak double-entry.
    """
    # ── Cek pending limit order dulu (cepat, tanpa API call) ─────────────────
    if symbol in pending_limit_orders:
        return True
    try:
        data = api_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
        for p in data:
            if float(p["positionAmt"]) != 0:
                return True
        return False
    except Exception as e:
        # Fallback ke memory jika API error — lebih aman daripada double entry
        print(f"  ⚠️  has_position API error {symbol}: {e} — fallback ke memory")
        return symbol in active_positions


def has_active_position(pair: str, trade_direction: str) -> bool:
    """
    Cek apakah sudah ada posisi aktif untuk pair + arah tertentu.
    trade_direction: 'BULLISH' atau 'BEARISH'

    Cek dari active_positions memory (cepat) + pending_limit_orders + validasi ke Binance.
    Mencegah double-entry di arah yang sama.
    """
    dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"

    # ── Cek pending limit order (belum terisi tapi slot sudah dipakai) ────────
    if pair in pending_limit_orders:
        pend_dir = pending_limit_orders[pair].get("direction", "")
        if pend_dir == dir_str:
            return True

    # ── Cek dari memory dulu (cepat) ─────────────────────────────────────────
    for sym, pos in active_positions.items():
        if sym == pair and pos.get("direction") == dir_str:
            return True

    # ── Cek langsung ke Binance untuk arah spesifik (hedge mode aware) ───────
    try:
        data = api_get("/fapi/v2/positionRisk", {"symbol": pair}, signed=True)
        for p in data:
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            # ONE-WAY mode: amt > 0 = LONG, amt < 0 = SHORT
            pos_side = p.get("positionSide", "BOTH")
            if pos_side == "BOTH":
                if dir_str == "LONG" and amt > 0:
                    return True
                if dir_str == "SHORT" and amt < 0:
                    return True
            else:
                # HEDGE mode: positionSide eksplisit
                if pos_side == dir_str:
                    return True
    except Exception as e:
        print(f"  ⚠️  has_active_position API error {pair}: {e} — fallback ke memory")
        # Fallback: kalau API error, cek memory saja
        return any(sym == pair and pos.get("direction") == dir_str
                   for sym, pos in active_positions.items())

    return False


def sync_closed_positions():
    """
    Scan semua active_positions dan bandingkan dengan posisi real di Binance.
    Jika posisi sudah tidak ada di Binance (close dari luar / TP-SL kena),
    hapus dari active_positions dan kirim notif ke Telegram.

    Dipanggil setiap scan loop — ringan karena hanya query 1x untuk semua posisi.
    """
    if not active_positions:
        return

    try:
        # Ambil semua posisi Binance sekali — lebih efisien dari query per symbol
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
        # ── Hedge mode fix: satu symbol bisa punya 2 row (LONG + SHORT) ──────
        # Simpan sebagai dict: symbol → total |positionAmt| dari semua sisi.
        # Selama SALAH SATU sisi masih open (amt != 0), posisi dianggap masih ada.
        live_positions: dict = {}
        for p in all_pos:
            sym = p["symbol"]
            amt = abs(float(p.get("positionAmt", 0)))
            live_positions[sym] = live_positions.get(sym, 0.0) + amt
    except Exception as e:
        print(f"  ⚠️  sync_closed_positions: gagal fetch positionRisk: {e}")
        return

    to_remove = []
    for symbol, pos in list(active_positions.items()):
        live_total = live_positions.get(symbol, 0.0)
        if live_total == 0.0:
            # Posisi sudah tidak ada di Binance — close dari luar
            # ── FIX BUG: duplicate-close guard ────────────────────────────────
            # Kalau symbol ini baru saja di-commit closed oleh manage_trailing()
            # atau sync ini sendiri, jangan langsung percaya positionRisk sekali
            # baca — verifikasi ulang ke Binance dulu (lihat penjelasan lengkap
            # di manage_trailing()). Mencegah symbol yang sama diproses closed
            # dua kali dalam window singkat.
            if _is_recently_closed(symbol) and not confirm_position_really_closed(symbol):
                print(f"  🛡️  [{symbol}] Duplicate-close guard (sync_closed_positions): "
                      f"posisi ternyata masih open — skip untuk sekarang.")
                continue
            to_remove.append(symbol)

    for symbol in to_remove:
        # FIX DOUBLE-LOG: claim atomik — kalau manage_trailing() sudah pop duluan,
        # skip (sebelumnya KeyError / close diproses dua kali → TP2 + BE ganda).
        with _positions_lock:
            pos = active_positions.pop(symbol, None)
        if pos is None:
            print(f"  🛡️  [{symbol}] Close sudah diproses thread lain (sync) — skip.")
            continue
        direction = pos["direction"]
        entry     = pos["entry"]
        sl_price  = pos["sl"]
        tp1_price = pos["tp1"]
        tp2_price = pos["tp2"]
        # FIX KRITIS: pakai harga FILL ASLI dari Binance, bukan mark price live
        # — lihat penjelasan lengkap di get_actual_close_price(). Tanpa ini,
        # PnL/label bisa salah total kalau harga sempat rebound sebelum sync
        # ini sempat jalan (mis. SL slip jauh lalu harga balik naik).
        price = get_actual_close_price(symbol)
        if price is None:
            price = get_current_price(symbol)
            if price is not None:
                print(f"  ⚠️  [{symbol}] Gagal ambil harga fill asli di sync_closed_positions — "
                      f"fallback ke mark price (PnL/label bisa kurang akurat).")

        # ── Bersihkan sisa open orders (SL/TP yang belum kena) ───────────────
        # Setelah posisi close (TP1 kena, SL kena, atau manual), order SL/TP
        # yang tersisa di Binance dibersihkan otomatis agar tidak nyampah.
        try:
            cancel_open_orders(symbol)
            print(f"  🧹 [{symbol}] Sisa open orders dibersihkan setelah posisi close.")
        except Exception as _cancel_err:
            print(f"  ⚠️  [{symbol}] Gagal cancel sisa open orders: {_cancel_err}")

        # ── Tentukan apakah close karena SL atau TP ──────────────────────────
        # Prioritas deteksi:
        # 1. Trailing BE aktif (tp1_hit=True, trailing_active=True) →
        #    sl sudah di entry, jadi price ≈ sl_price BUKAN SL murni, ini trailing win
        # 2. Price dekat TP → TP hit
        # 3. Price dekat SL (original) → SL hit
        # 4. Ambiguous → cek jarak
        is_sl_hit     = False
        is_trailing_be = pos.get("tp1_hit", False) and pos.get("trailing_active", False)

        if price is not None:
            sl_dist  = abs(price - sl_price)  / max(sl_price,  1e-9)
            tp1_dist = abs(price - tp1_price) / max(tp1_price, 1e-9)

            if direction == "LONG":
                if is_trailing_be and price >= entry * 0.998:
                    # Trailing BE close: harga di atas atau sangat dekat entry → bukan SL
                    is_sl_hit = False
                elif price <= sl_price * 1.005:
                    is_sl_hit = True
                elif price >= tp1_price * 0.995:
                    is_sl_hit = False
                else:
                    is_sl_hit = sl_dist < tp1_dist
            else:  # SHORT
                if is_trailing_be and price <= entry * 1.002:
                    # Trailing BE close: harga di bawah atau sangat dekat entry → bukan SL
                    is_sl_hit = False
                elif price >= sl_price * 0.995:
                    is_sl_hit = True
                elif price <= tp1_price * 1.005:
                    is_sl_hit = False
                else:
                    is_sl_hit = sl_dist < tp1_dist

        # Tentukan hasil trade
        pnl = 0.0  # FIX A: inisialisasi agar tidak NameError jika price is None
        if price is not None:
            # Kalau TP1 sudah kena sebelumnya, lot yang tersisa hanya tp2_size.
            # Pakai full lot hanya kalau TP1 belum pernah kena.
            _close_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos["lot"]
            if not _close_lot:
                _close_lot = pos["lot"]
            # BE = TP1 sudah kena + bukan SL murni → dihitung WIN meski sisa lot close di entry
            _is_be = pos.get("tp1_hit", False) and not is_sl_hit
            if direction == "LONG":
                pnl    = (price - entry) * _close_lot
                is_win = _is_be or price > entry
            else:
                pnl    = (entry - price) * _close_lot
                is_win = _is_be or price < entry
            # BE close: pastikan PnL sedikit positif agar update_performance hitung WIN
            if _is_be and pnl <= 0:
                pnl = 0.001
            pnl_em  = "🟢" if pnl >= 0 else "🔴"
            pnl_str = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT"
        else:
            pnl_str = "N/A"
            is_win  = False
            is_sl_hit = False  # tidak bisa tentukan, asumsikan bukan SL

        _perf_pnl = pnl if price is not None else (1 if is_win else -1)
        # FIX Bug 5: update_performance SEBELUM notify agar stats di notif sudah terupdate
        update_performance(_perf_pnl)
        with _positions_lock:
            active_positions.pop(symbol, None)
        _mark_position_closed(symbol)   # FIX BUG: duplicate-close guard — catat waktu commit
        save_state()   # FIX: sinkron ke disk begitu posisi close terdeteksi (sync_closed_positions)

        if is_sl_hit:
            set_cooldown_post_close(symbol, reason="SL hit terdeteksi", is_stoploss=True)
            record_pair_sl(symbol)
        elif is_trailing_be:
            set_cooldown_post_close(symbol, reason="Trailing BE close", is_stoploss=False)
        else:
            set_cooldown_post_close(symbol, reason="TP/manual close", is_stoploss=False)

        print(f"  🔄 [{symbol}] Posisi close terdeteksi dari luar | PnL: {pnl_str} | {'SL HIT' if is_sl_hit else 'TP/Manual'}")

        # ── Kirim notifikasi — update_performance sudah dipanggil, stats fresh ──
        _close_price = price if price is not None else sl_price
        if is_sl_hit:
            notify_sl_hit(symbol, pos, _close_price, "Stop Loss")
        elif is_trailing_be:
            notify_sl_hit(symbol, pos, price if price is not None else entry, "Trailing Stop (Break Even)")
        else:
            _cur_price = price if price is not None else tp1_price
            if pos.get("tp1_hit"):
                notify_tp2_hit(symbol, pos, _cur_price)
            else:
                notify_tp1_hit(symbol, pos, _cur_price)

    if to_remove:
        print(f"  🔄 sync_closed_positions: {len(to_remove)} posisi dihapus dari memory → {to_remove}")

def count_open_positions():
    """
    Hitung posisi aktif dari Binance + active_positions memory.
    Pakai max keduanya untuk handle race condition:
    posisi yang baru dibuka mungkin belum muncul di positionRisk Binance.
    """
    try:
        data = api_get("/fapi/v2/positionRisk", signed=True)
        binance_count = sum(1 for p in data if float(p["positionAmt"]) != 0)
    except Exception:
        binance_count = 0
    memory_count = len(active_positions)
    # Pakai nilai terbesar — lebih aman daripada under-count
    return max(binance_count, memory_count)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 5b — SINKRONISASI POSISI PRE-EXISTING
# ═══════════════════════════════════════════════════════════════════════════
#
#  Posisi yang sudah buka SEBELUM bot di-start akan di-import ke
#  active_positions sehingga:
#  - Ikut dihitung di hourly report & /pnl
#  - Trailing stop & TP monitor tetap berjalan
#  - /closeallposition bisa menutupnya
#
#  Data yang tidak diketahui (entry asli, SL/TP) diisi dari Binance
#  positionRisk + openOrders supaya semaksimal mungkin akurat.
# ═══════════════════════════════════════════════════════════════════════════

def sync_existing_positions():
    """
    Import semua posisi yang sudah ada di Binance ke active_positions.
    Dipanggil sekali saat bot start, setelah init_balance().
    """
    try:
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
    except Exception as e:
        print(f"⚠️ Gagal sync posisi existing: {e}")
        return

    imported = []
    for p in all_pos:
        symbol = p["symbol"]
        amt    = float(p["positionAmt"])
        if amt == 0:
            continue
        if symbol in active_positions:
            # FIX KRITIS: jangan cuma skip diam-diam. Posisi ini sudah dikenal bot
            # (mis. tp1_hit=True dari load_state()), tapi belum tentu SL-nya masih
            # benar-benar terpasang di Binance saat ini — bisa saja sempat gagal
            # dipasang ulang (SL BE / trailing) tepat sebelum restart terjadi.
            # Verifikasi langsung ke exchange sebagai jaring pengaman independen,
            # supaya bot tidak pernah diam-diam menganggap posisi aman padahal
            # naked, walau flag unprotected di state entah kenapa tidak ter-set.
            try:
                _existing_orders = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
                _has_sl = any(o.get("type") in ("STOP_MARKET", "STOP") for o in _existing_orders)
            except Exception:
                _has_sl = True  # gagal cek → jangan asumsikan naked, biar tidak salah alert
            if not _has_sl:
                # Cek juga algoOrder (mainnet) sebelum menyimpulkan benar-benar tanpa SL
                try:
                    _algo_resp = api_get("/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True)
                    _algo_list = _algo_resp if isinstance(_algo_resp, list) else _algo_resp.get("orders", [])
                    _has_sl = any(o.get("type") in ("STOP_MARKET", "STOP") for o in _algo_list)
                except Exception:
                    pass  # testnet 404 atau gagal fetch — _has_sl tetap dari pengecekan di atas
            if not _has_sl:
                with _positions_lock:
                    active_positions[symbol]["unprotected"] = True
                save_state()
                print(f"  🚨 [{symbol}] Posisi sudah dikenal tapi TANPA SL terverifikasi di Binance — ditandai unprotected untuk retry segera")
                send_telegram_raw(
                    f"🚨 <b>POSISI TANPA SL TERDETEKSI SAAT SYNC — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Posisi ini sudah dikenal bot tapi tidak ada order SL aktif di Binance.\n"
                    f"⚠️ Bot akan retry pasang SL otomatis di scan berikutnya."
                )
            continue  # tetap skip re-import — data lain (entry/tp1_hit/dst) sudah benar dari state
        if symbol in pending_limit_orders:
            # Posisi ini dari limit order bot yang sudah FILLED — jangan import sebagai HP.
            # monitor_pending_limit_orders() akan aktifkan posisi + pasang SL/TP di scan berikutnya.
            print(f"  ⏭  [{symbol}] Skip import HP — masih ada di pending_limit_orders (akan diaktifkan monitor loop)")
            continue

        direction  = "LONG" if amt > 0 else "SHORT"
        side       = "BUY" if direction == "LONG" else "SELL"
        sl_side    = "SELL" if side == "BUY" else "BUY"
        entry      = float(p.get("entryPrice", 0))
        mark_price = float(p.get("markPrice",  entry))
        liq_price  = float(p.get("liquidationPrice", 0))
        lot        = abs(amt)

        # Coba ambil SL & TP dari open orders — cek BOTH regular + algo endpoint
        sl_price  = 0.0
        tp1_price = 0.0
        tp2_price = 0.0
        try:
            orders = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
            stop_orders = [o for o in orders if o["type"] in ("STOP_MARKET", "STOP")]
            tp_orders   = [o for o in orders if o["type"] in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")]
            if stop_orders:
                sl_price = float(stop_orders[0].get("stopPrice", 0))
            if len(tp_orders) >= 1:
                tp1_price = float(tp_orders[0].get("stopPrice", 0))
            if len(tp_orders) >= 2:
                tp2_price = float(tp_orders[1].get("stopPrice", 0))
        except Exception:
            pass

        # ── Juga cek algoOrder (SL/TP yang dikirim via /fapi/v1/algoOrder) ──
        # Mainnet saja — testnet tidak support endpoint ini (404)
        if sl_price == 0.0 or tp1_price == 0.0:
            try:
                algo_resp = api_get("/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True)
                algo_list = algo_resp if isinstance(algo_resp, list) else algo_resp.get("orders", [])
                algo_stops = [o for o in algo_list if o.get("type") in ("STOP_MARKET", "STOP")]
                algo_tps   = [o for o in algo_list if o.get("type") in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")]
                if sl_price == 0.0 and algo_stops:
                    sl_price = float(algo_stops[0].get("triggerPrice", 0) or algo_stops[0].get("stopPrice", 0))
                if tp1_price == 0.0 and len(algo_tps) >= 1:
                    tp1_price = float(algo_tps[0].get("triggerPrice", 0) or algo_tps[0].get("stopPrice", 0))
                if tp2_price == 0.0 and len(algo_tps) >= 2:
                    tp2_price = float(algo_tps[1].get("triggerPrice", 0) or algo_tps[1].get("stopPrice", 0))
            except Exception:
                pass  # testnet → 404, skip saja

        # ── Single TP: kalau hanya ditemukan satu TP, itu adalah target final.
        # Jangan membuat TP kedua/fallback berbeda.
        if tp1_price > 0 and tp2_price == 0:
            tp2_price = tp1_price

        # Fallback SL & TP kalau open orders kosong
        if sl_price == 0:
            # Estimasi SL dari liquidation price (lebih aman)
            if liq_price > 0:
                sl_price = liq_price
            elif direction == "LONG":
                sl_price = entry * 0.97
            else:
                sl_price = entry * 1.03

        if tp1_price == 0:
            tp1_price = (entry * 1.015) if direction == "LONG" else (entry * 0.985)
        if tp2_price == 0:
            tp2_price = tp1_price if tp1_price > 0 else ((entry * 1.015) if direction == "LONG" else (entry * 0.985))

        # ── GUARD SANITY: SL/TP absurd dari order Binance yang di-adopsi ─────
        # sl_price/tp1_price/tp2_price di atas bisa datang APA ADANYA dari open
        # order di exchange (order lama/manual/nyasar) tanpa pernah divalidasi.
        # Kalau harga <= 0 (mustahil) atau jaraknya dari entry jauh di luar akal
        # (>3× MAX_SL_DISTANCE_PCT — toleransi lebih lebar dari sinyal bot sendiri
        # karena posisi manual boleh pakai stop lebih longgar, tapi tetap ada batas),
        # order itu dianggap tidak valid → fallback ke estimasi aman + alert user.
        # Ini nutup celah yang bikin TP2 bisa jadi NEGATIF (harga mustahil) kalau
        # SL asli udah kejauhan dari entry.
        _anomaly_reason = None
        if sl_price <= 0 or (entry > 0 and abs(entry - sl_price) / entry > MAX_SL_DISTANCE_PCT * 3):
            _anomaly_reason = f"SL={sl_price} tidak wajar (jarak {abs(entry-sl_price)/max(entry,1e-9)*100:.1f}% dari entry)"
            sl_price = entry * 0.97 if direction == "LONG" else entry * 1.03
        if tp1_price <= 0 or (direction == "LONG" and tp1_price <= entry) or (direction == "SHORT" and tp1_price >= entry):
            _anomaly_reason = (_anomaly_reason + " | " if _anomaly_reason else "") + f"TP1={tp1_price} tidak wajar"
            tp1_price = (entry * 1.015) if direction == "LONG" else (entry * 0.985)
        if tp2_price <= 0 or (direction == "LONG" and tp2_price <= tp1_price) or (direction == "SHORT" and tp2_price >= tp1_price):
            _anomaly_reason = (_anomaly_reason + " | " if _anomaly_reason else "") + f"TP2={tp2_price} tidak wajar"
            tp2_price = (entry * 1.025) if direction == "LONG" else (entry * 0.975)
        if _anomaly_reason:
            print(f"  ⚠️  [{symbol}] SL/TP anomaly terdeteksi saat adopsi posisi pre-existing: {_anomaly_reason} → di-override ke estimasi aman")
            send_telegram_raw(
                f"⚠️ <b>SL/TP TIDAK WAJAR — {symbol}</b>\n"
                f"{'─'*34}\n"
                f"{_anomaly_reason}\n"
                f"Bot override ke estimasi aman: SL={sl_price:.6f} TP1={tp1_price:.6f} TP2={tp2_price:.6f}\n"
                f"⚠️ Ini cuma estimasi — cek & sesuaikan manual SL/TP di exchange kalau perlu."
            )

        # ── FIX RISIKO 3: Cek apakah SL (estimasi atau dari open orders) mewakili
        # loss yang melebihi MAX_SL_LOSS_PCT × balance. Jika ya, kirim peringatan
        # agar user aware — bot tidak bisa ubah lot posisi yang sudah terbuka.
        try:
            _pre_bal = get_total_balance()
            if _pre_bal and _pre_bal > 0 and entry > 0 and sl_price > 0:
                _pre_sl_dist  = abs(entry - sl_price)
                _pre_loss_est = lot * _pre_sl_dist
                _pre_max_loss = _pre_bal * MAX_SL_LOSS_PCT
                if _pre_loss_est > _pre_max_loss * 1.5:   # toleransi 50% sebelum alert
                    _pre_sl_pct = (_pre_sl_dist / entry) * 100
                    print(
                        f"  ⚠️  [{symbol}] Pre-existing: estimasi loss jika SL kena "
                        f"{_pre_loss_est:.2f} USDT > max {_pre_max_loss:.2f} USDT "
                        f"(SL {_pre_sl_pct:.2f}% dari entry, lot={lot})"
                    )
                    send_telegram_raw(
                        f"⚠️ <b>RISIKO TINGGI — POSISI PRE-EXISTING: {symbol}</b>\n"
                        f"{'─'*34}\n"
                        f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry} | Lot: {lot}\n"
                        f"🛑 SL estimasi: <b>{sl_price}</b> ({_pre_sl_pct:.2f}% dari entry)\n"
                        f"💸 Estimasi loss jika SL kena: <b>{_pre_loss_est:.2f} USDT</b>\n"
                        f"🎯 Max loss per trade (setmarginratio): <b>{_pre_max_loss:.2f} USDT</b> "
                        f"({MAX_SL_LOSS_PCT*100:.2f}%)\n"
                        f"{'─'*34}\n"
                        f"⚠️ Posisi ini dibuka sebelum bot aktif — bot tidak bisa ubah lot.\n"
                        f"Pertimbangkan untuk kurangi lot atau perbaiki SL secara manual!"
                    )
        except Exception as _pre_risk_err:
            print(f"  ⚠️  [{symbol}] Gagal cek risiko pre-existing: {_pre_risk_err}")

        # Ambil tick & step size
        filters      = get_lot_filters(symbol)
        tick_size    = filters.get("tickSize",     0.01)
        step_size    = filters.get("stepSize",     0.001)
        mkt_step     = filters.get("mkt_stepSize", step_size)
        min_qty      = filters.get("minQty",       0.001)
        qty_prec     = filters.get("quantityPrecision", None)

        tp1_size = round_lot_to_step(lot * TP1_PARTIAL, mkt_step)   # FIX: pakai mkt_step bukan step_size
        tp2_size = round_lot_to_step(lot - tp1_size,    mkt_step)

        # ── Flag: apakah perlu pasang SL/TP baru? ────────────────────────────
        needs_sl = (sl_price == 0.0)
        needs_tp = (tp1_price == 0.0)

        with _positions_lock:   # FIX BUG #3: thread-safe write
            active_positions[symbol] = {
                "entry":          entry,
                "sl":             sl_price,
                "original_sl":    sl_price,  # FIX: simpan SL awal untuk kalkulasi RR trailing
                "tp1":            tp1_price,
                "tp2":            tp2_price,
                "lot":            lot,
                "tp1_size":       tp1_size,
                "tp2_size":       tp2_size,
                "side":           side,
                "sl_side":        sl_side,
                "direction":      direction,
                "position_side":  "BOTH",   # default; hedge mode akan override saat monitor
                "tick_size":      tick_size,
                "step_size":      step_size,
                "mkt_step_size":  mkt_step,
                "min_qty":        min_qty,
                "eff_prec":       max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4,
                "tp1_hit":        False,
                "trailing_active": False,
                "open_time":      None,   # waktu asli tidak diketahui
                "pre_existing":   True,   # flag — masuk sebelum bot start
                # FIX: konsistensi dengan _activate_position_from_limit — posisi
                # pre-existing tidak punya mode/entry_tf/score asli (dibuka manual
                # atau sebelum bot start), tandai jelas alih-alih silently "?".
                "mode":           "IMPORTED",
                "entry_tf":       "?",
                "score":          0,
            }
        imported.append(symbol)
        print(f"  📥 Import posisi existing: {symbol} {direction} | Entry:{entry} | Lot:{lot} | sl={'ada' if not needs_sl else '❌ KOSONG'}")

        # ── Auto pasang SL (dan TP) jika tidak ada — posisi naked bahaya! ────
        if needs_sl:
            print(f"  🚨 [{symbol}] Tidak ada SL — akan dipasang otomatis...")
            try:
                mark_now  = get_current_price(symbol) or entry
                # Hitung SL dari mark price sekarang (bukan entry lama) agar tidak
                # langsung kena kalau harga sudah bergerak jauh
                if direction == "LONG":
                    auto_sl = round_price_to_tick(min(sl_price if sl_price > 0 else entry * 0.97,
                                                      mark_now * 0.985), tick_size)
                else:
                    auto_sl = round_price_to_tick(max(sl_price if sl_price > 0 else entry * 1.03,
                                                      mark_now * 1.015), tick_size)

                if qty_prec is not None:
                    _fix_lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
                else:
                    _fix_lot = lot

                algo_post_sl(symbol, sl_side, auto_sl, quantity=_fix_lot, position_side="BOTH")
                active_positions[symbol]["sl"] = auto_sl
                print(f"  ✅ [{symbol}] Auto-SL dipasang di {auto_sl}")
                send_telegram_raw(
                    f"🛡️ <b>AUTO-SL DIPASANG — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Posisi ditemukan tanpa SL saat bot restart.\n"
                    f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry}\n"
                    f"🛑 SL otomatis dipasang di: <b>{auto_sl}</b>\n"
                    f"Mark price saat ini: {mark_now}"
                )
            except Exception as _sl_err:
                print(f"  ❌ [{symbol}] Gagal auto-pasang SL: {_sl_err}")
                send_telegram_raw(
                    f"🆘 <b>POSISI TANPA SL — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Bot gagal pasang SL otomatis!\n"
                    f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry} | Lot: {lot}\n"
                    f"⚠️ Cek dan pasang SL manual di Binance!\n"
                    f"Error: <code>{_sl_err}</code>"
                )

        if needs_tp:
            try:
                mark_now = get_current_price(symbol) or entry
                sl_dist  = abs((active_positions[symbol]["sl"] or entry) - entry)
                if sl_dist == 0:
                    sl_dist = entry * 0.02  # fallback 2%
                if direction == "LONG":
                    auto_target = round_price_to_tick(entry + sl_dist * 2.5, tick_size)
                else:
                    auto_target = round_price_to_tick(entry - sl_dist * 2.5, tick_size)

                if qty_prec is not None:
                    _fix_lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
                else:
                    _fix_lot = lot
                _tp_sz = round_lot_to_step(_fix_lot, mkt_step)
                if _tp_sz < min_qty: _tp_sz = round_lot_to_step(min_qty, mkt_step)
                _tp_sz = min(_tp_sz, _fix_lot)

                _auto_tp_ref = algo_post_tp(symbol, sl_side, auto_target, _tp_sz, position_side="BOTH")
                active_positions[symbol]["tp1"] = auto_target
                active_positions[symbol]["tp2"] = auto_target
                active_positions[symbol]["tp1_size"] = _tp_sz
                active_positions[symbol]["tp2_size"] = 0.0
                active_positions[symbol]["tp1_order_ref"] = _auto_tp_ref
                active_positions[symbol]["tp2_order_ref"] = None
                print(f"  ✅ [{symbol}] Auto-TP tunggal dipasang: target={auto_target} | lot={_tp_sz}")
            except Exception as _tp_err:
                print(f"  ⚠️  [{symbol}] Gagal auto-pasang TP: {_tp_err}")

    if imported:
        msg_lines = [f"  • {s} {active_positions[s]['direction']} | Entry: {active_positions[s]['entry']}" for s in imported]
        msg = (
            f"📥 <b>Posisi Pre-Existing Terdeteksi</b>\n"
            f"{'─'*38}\n"
            f"Bot menemukan <b>{len(imported)}</b> posisi yang sudah terbuka:\n"
            + "\n".join(msg_lines) + "\n"
            f"{'─'*38}\n"
            f"ℹ️ Posisi ini ikut dihitung di /pnl dan hourly report."
        )
        send_telegram_raw(msg)
        print(f"✅ {len(imported)} posisi pre-existing di-import.")
    else:
        print("✅ Tidak ada posisi pre-existing.")

# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 4 — EMA TREND FILTER
# ═══════════════════════════════════════════════════════════════════════════

def calculate_ema(series: pd.Series, period: int) -> float:
    if len(series) < period:
        return float(series.iloc[-1])
    ema = series.ewm(span=period, adjust=False).mean()
    return float(ema.iloc[-1])


def get_ema_score(df: pd.DataFrame, direction: str) -> tuple:
    closes = df["close"]
    ema20  = calculate_ema(closes, 20)
    ema50  = calculate_ema(closes, 50)
    ema200 = calculate_ema(closes, 200)
    price  = float(closes.iloc[-1])

    if direction == "BULLISH":
        short_aligned = ema20 > ema50
        long_aligned  = ema50 > ema200
    else:
        short_aligned = ema20 < ema50
        long_aligned  = ema50 < ema200

    score = 0
    parts = []
    if short_aligned:
        score += SCORE_EMA_ALIGNED
        parts.append(f"EMA20{'>' if direction == 'BULLISH' else '<'}EMA50 ✅")
    if long_aligned:
        score += SCORE_EMA_BONUS
        parts.append(f"EMA50{'>' if direction == 'BULLISH' else '<'}EMA200 ✅")
    if not short_aligned and not long_aligned:
        parts.append("EMA tidak align ⚠️")

    reason = " | ".join(parts) if parts else "EMA neutral"
    return score, reason, ema20, ema50, ema200


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 5 — MARKET STRUCTURE (BOS / CHoCH)
# ═══════════════════════════════════════════════════════════════════════════

# (Deteksi struktur reversal DIHAPUS — 1:1 main49: posisi/limit order tidak lagi
# ditutup/dibatalkan karena BOS/CHoCH berlawanan; hanya SL/TP/trailing yang menutup.)

def find_swings(df: pd.DataFrame, window: int = SWING_WINDOW) -> tuple:
    highs, lows = [], []
    for i in range(window, len(df) - window):
        hi_window = df["high"].iloc[i - window: i + window + 1]
        lo_window = df["low"].iloc[i - window: i + window + 1]
        if df["high"].iloc[i] == hi_window.max():
            highs.append((i, float(df["high"].iloc[i])))
        if df["low"].iloc[i] == lo_window.min():
            lows.append((i, float(df["low"].iloc[i])))
    return highs, lows


def _lux_order_blocks(df: pd.DataFrame, size: int = 5, swing_size: int = 50, atr_len: int = 200) -> list:
    """
    Order block internal gaya LuxAlgo "Smart Money Concepts" (OB belum termitigasi di bar terakhir).
    Return list of dict(top, bot, bias[+1/-1], idx) urut dari yang terbaru.
    Logika: leg/pivot (size 5), struktur jebol = close cross pivot terakhir; OB = candle paling ekstrem
    (parsed low terendah utk bullish / parsed high tertinggi utk bearish) dari bar pivot sampai bar sebelum jebol;
    mitigasi: bearish dihapus kalau high > OB high, bullish dihapus kalau low < OB low.
    Catatan: filter high-volatility bar (range >= 2 x ATR200) ikut ditiru; kondisi 'internal != swing pivot' ikut.
    """
    import numpy as np
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
        """per bar t: (event, level, idx) event: +1 pivot low baru, -1 pivot high baru, 0 tidak ada."""
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
    ih = il = sh = sl_ = None            # level
    ih_cr = il_cr = False
    ih_idx = il_idx = None
    prev_ih = prev_il = None
    obs = []
    for t in range(n):
        e, lvl, idx = ev_s[t]
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
                    obs.insert(0, {"top": PH[j], "bot": PL[j], "bias": 1, "idx": j})
            if il is not None and prev_il is not None and C[t] < il and C[t - 1] >= prev_il and not il_cr and il != sl_:
                il_cr = True
                seg = PH[il_idx:t]
                if len(seg):
                    j = il_idx + int(np.argmax(seg))
                    obs.insert(0, {"top": PH[j], "bot": PL[j], "bias": -1, "idx": j})
        # mitigasi
        obs = [o for o in obs if not ((o["bias"] == -1 and H[t] > max(o["top"], o["bot"])) or (o["bias"] == 1 and L[t] < min(o["top"], o["bot"])))]
        if len(obs) > 100:
            obs = obs[:100]
        prev_ih, prev_il = ih, il
    return obs


def _lux_retest_zone(df: pd.DataFrame, direction: str, price: float):
    """OB gaya LuxAlgo di sisi retest: BEARISH → supply di atas harga, BULLISH → demand di bawah harga.
    RZ_OB_PICK=0: terdekat; =1: OB asal impuls (pergerakan lanjutan terbesar setelah OB)."""
    obs = _lux_order_blocks(df, RZ_OB_SIZE)
    Hh = df["high"].astype(float).values
    Ll = df["low"].astype(float).values
    cands = []
    for o in obs:
        lo_, hi_ = min(o["top"], o["bot"]), max(o["top"], o["bot"])
        if direction == "BEARISH" and o["bias"] == -1 and hi_ >= price:
            d = max(0.0, lo_ - price)
            disp = hi_ - float(Ll[o["idx"] + 1:].min()) if o["idx"] + 1 < len(Ll) else 0.0
        elif direction == "BULLISH" and o["bias"] == 1 and lo_ <= price:
            d = max(0.0, price - hi_)
            disp = float(Hh[o["idx"] + 1:].max()) - lo_ if o["idx"] + 1 < len(Hh) else 0.0
        else:
            continue
        cands.append((d, disp, lo_, hi_))
    if not cands:
        return None
    if RZ_OB_PICK == 1:
        best = max(cands, key=lambda c: c[1])
    else:
        best = min(cands, key=lambda c: c[0])
    return best[2], best[3]


def _htf_demand_long_check(pair: str, entry_tf: str):
    """
    Cek setup LONG di demand TF tinggi. Return (zone_lo, zone_hi, wick_low) atau None.
    Zona = OB bullish gaya LuxAlgo (belum termitigasi) di 1D (mode 1) / 1W (mode 2) yang disentuh dalam
    DZ_TOUCH_BARS candle entry TF terakhir, dan harga sekarang masih di atas dasar zona.
    """
    try:
        DZ_STATS["checks"] += 1
        tf = "1w" if HTF_DEMAND_MODE == 2 else "1d"
        df_z = fetch_ohlcv_safe(pair, tf, limit=300)
        if df_z is None or len(df_z) < 40:
            DZ_STATS["fetch_fail"] += 1
            return None
        df_e = fetch_ohlcv(pair, entry_tf, limit=250)
        if df_e is None or len(df_e) < DZ_TOUCH_BARS + 5:
            return None
        obs = [o for o in _lux_order_blocks(df_z, RZ_OB_SIZE) if o["bias"] == 1]
        if not obs:
            DZ_STATS["no_bull_ob"] += 1
            return None
        recent = df_e.iloc[-DZ_TOUCH_BARS:]
        lows = recent["low"].astype(float).values
        highs = recent["high"].astype(float).values
        last_close = float(df_e["close"].iloc[-1])
        for o in obs:   # terbaru dulu
            z_lo, z_hi = min(o["top"], o["bot"]), max(o["top"], o["bot"])
            touched = bool(((lows <= z_hi) & (highs >= z_lo)).any())
            if touched and last_close > z_lo:
                DZ_STATS["touched"] += 1
                return float(z_lo), float(z_hi), float(lows.min())
        return None
    except Exception:
        return None


def _demand_buyer_reaction(df: pd.DataFrame, z_lo: float, z_hi: float, atr: float) -> tuple:
    """
    Reaksi buyer di zona demand → (ok: bool, alasan: str). Hanya candle yang SUDAH close.
    Syarat (semua harus ya):
      1) candle close terakhir bullish (close > open)
      2) harga terakhir masih di atas dasar zona
      3) ada reaksi buyer sejak sentuhan pertama zona: ekor bawah panjang di candle yang masuk zona,
         ATAU harga sudah naik >= DZ_REACTION_BOUNCE_ATR x ATR dari titik terendah.
    Tidak butuh BOS / HH+HL.
    """
    try:
        rec = df.iloc[-DZ_TOUCH_BARS:]
        o = [float(x) for x in rec["open"].values]
        h = [float(x) for x in rec["high"].values]
        lo = [float(x) for x in rec["low"].values]
        c = [float(x) for x in rec["close"].values]
        n = len(c)
        if n < 2:
            return False, "data kurang"
        if not (c[-1] > o[-1]):
            return False, "candle close terakhir belum bullish"
        if not (c[-1] > z_lo):
            return False, "harga di bawah dasar zona"
        touched = [i for i in range(n) if lo[i] <= z_hi and h[i] >= z_lo]
        if not touched:
            return False, "tidak ada sentuhan zona"
        fi = touched[0]
        best_wick = 0.0
        for i in touched:
            rng = h[i] - lo[i]
            if rng > 0 and c[i] > z_lo and (not atr or atr <= 0 or rng >= DZ_REACTION_MIN_RANGE_ATR * atr):
                best_wick = max(best_wick, (min(o[i], c[i]) - lo[i]) / rng)
        bounce = c[-1] - min(lo[fi:])
        wick_ok = best_wick >= DZ_REACTION_WICK_MIN
        bounce_ok = bool(atr and atr > 0 and bounce >= DZ_REACTION_BOUNCE_ATR * atr)
        if not (wick_ok or bounce_ok):
            return False, "belum ada reaksi buyer (ekor bawah pendek & belum mantul)"
        why = []
        if wick_ok:
            why.append(f"ekor bawah {best_wick * 100:.0f}% di zona")
        if bounce_ok:
            why.append(f"mantul {bounce / atr:.1f}xATR dari low")
        return True, "buyer bereaksi (" + ", ".join(why) + ") + candle close bullish"
    except Exception as e:  # noqa: BLE001
        return False, f"error {e}"


def _htf_supply_short_check(pair: str, entry_tf: str):
    """
    Cek setup SHORT di supply TF tinggi. Return (zone_lo, zone_hi, wick_high) atau None.
    Zona = OB bearish gaya LuxAlgo (zona merah, belum termitigasi) di TF sesuai HTF_SUPPLY_MODE yang disentuh
    dalam DZ_TOUCH_BARS candle entry TF terakhir, dan harga sekarang masih di bawah atap zona.
    """
    try:
        SZ_STATS["checks"] += 1
        tf = SZ_TF_MAP.get(HTF_SUPPLY_MODE, "1h")
        df_z = fetch_ohlcv_safe(pair, tf, limit=300)
        if df_z is None or len(df_z) < 40:
            SZ_STATS["fetch_fail"] += 1
            return None
        df_e = fetch_ohlcv(pair, entry_tf, limit=250)
        if df_e is None or len(df_e) < DZ_TOUCH_BARS + 5:
            return None
        obs = [o for o in _lux_order_blocks(df_z, RZ_OB_SIZE) if o["bias"] == -1]
        if not obs:
            SZ_STATS["no_bear_ob"] += 1
            return None
        recent = df_e.iloc[-DZ_TOUCH_BARS:]
        lows = recent["low"].astype(float).values
        highs = recent["high"].astype(float).values
        last_close = float(df_e["close"].iloc[-1])
        for o in obs:   # terbaru dulu
            z_lo, z_hi = min(o["top"], o["bot"]), max(o["top"], o["bot"])
            touched = bool(((lows <= z_hi) & (highs >= z_lo)).any())
            if touched and last_close < z_hi:
                SZ_STATS["touched"] += 1
                return float(z_lo), float(z_hi), float(highs.max())
        return None
    except Exception:
        return None


def _supply_seller_reaction(df: pd.DataFrame, z_lo: float, z_hi: float, atr: float) -> tuple:
    """
    Reaksi seller di zona supply → (ok: bool, alasan: str). Hanya candle yang SUDAH close. Kembaran _demand_buyer_reaction.
    Syarat (semua harus ya):
      1) candle close terakhir bearish (close < open)
      2) harga terakhir masih di bawah atap zona
      3) ada reaksi seller sejak sentuhan pertama zona: ekor ATAS panjang di candle yang masuk zona,
         ATAU harga sudah turun >= DZ_REACTION_BOUNCE_ATR x ATR dari titik tertinggi.
    Tidak butuh BOS / LH+LL.
    """
    try:
        rec = df.iloc[-DZ_TOUCH_BARS:]
        o = [float(x) for x in rec["open"].values]
        h = [float(x) for x in rec["high"].values]
        lo = [float(x) for x in rec["low"].values]
        c = [float(x) for x in rec["close"].values]
        n = len(c)
        if n < 2:
            return False, "data kurang"
        if not (c[-1] < o[-1]):
            return False, "candle close terakhir belum bearish"
        if not (c[-1] < z_hi):
            return False, "harga di atas atap zona"
        touched = [i for i in range(n) if lo[i] <= z_hi and h[i] >= z_lo]
        if not touched:
            return False, "tidak ada sentuhan zona"
        fi = touched[0]
        best_wick = 0.0
        for i in touched:
            rng = h[i] - lo[i]
            if rng > 0 and c[i] < z_hi and (not atr or atr <= 0 or rng >= DZ_REACTION_MIN_RANGE_ATR * atr):
                best_wick = max(best_wick, (h[i] - max(o[i], c[i])) / rng)
        drop = max(h[fi:]) - c[-1]
        wick_ok = best_wick >= DZ_REACTION_WICK_MIN
        drop_ok = bool(atr and atr > 0 and drop >= DZ_REACTION_BOUNCE_ATR * atr)
        if not (wick_ok or drop_ok):
            return False, "belum ada reaksi seller (ekor atas pendek & belum turun)"
        why = []
        if wick_ok:
            why.append(f"ekor atas {best_wick * 100:.0f}% di zona")
        if drop_ok:
            why.append(f"turun {drop / atr:.1f}xATR dari high")
        return True, "seller bereaksi (" + ", ".join(why) + ") + candle close bearish"
    except Exception as e:  # noqa: BLE001
        return False, f"error {e}"


def _sz_entry_sl_tp(price: float, z_hi: float, wick_high: float, atr: float) -> tuple:
    """Entry SHORT di harga sekarang, SL di atas atap zona / wick tertinggi sentuhan + buffer."""
    entry = price
    sl = max(z_hi, wick_high) + RZ_SL_BUF_ATR * (atr or 0.0)
    dist = sl - entry
    tp1 = entry - dist * TP1_R_MULT
    tp2 = entry - dist * (RR_GRADE_A + 0.5)
    if dist <= 0:
        return entry, sl, tp1, tp2, 0.0, 0.0
    if SINGLE_TP_MODE:
        tp1 = entry - dist * SINGLE_TP_R_MULT
        tp2 = tp1
    return entry, sl, tp1, tp2, round(abs(tp1 - entry) / dist, 2), round(abs(tp2 - entry) / dist, 2)


def _dz_entry_sl_tp(price: float, z_lo: float, wick_low: float, atr: float) -> tuple:
    """Entry LONG di harga sekarang, SL di bawah dasar zona / wick sentuhan terendah + buffer."""
    entry = price
    sl = min(z_lo, wick_low) - RZ_SL_BUF_ATR * (atr or 0.0)
    dist = entry - sl
    tp1 = entry + dist * TP1_R_MULT
    tp2 = entry + dist * (RR_GRADE_A + 0.5)
    if dist <= 0:
        return entry, sl, tp1, tp2, 0.0, 0.0
    if SINGLE_TP_MODE:
        tp1 = entry + dist * SINGLE_TP_R_MULT
        tp2 = tp1
    return entry, sl, tp1, tp2, round(abs(tp1 - entry) / dist, 2), round(abs(tp2 - entry) / dist, 2)


def _htf_retest_zone(df_htf: pd.DataFrame, direction: str):
    """
    Zona retest HTF → (lo, hi) atau None.
    BEARISH: swing low 1h (pivot 3/3) yang sudah ditembus close ke bawah dan levelnya di ATAS close terakhir
             (support jebol jadi resistance); pilih yang terdekat dari harga.
    BULLISH: swing high 1h yang sudah ditembus close ke atas dan levelnya di BAWAH close terakhir.
    """
    try:
        n = len(df_htf)
        if n < 30:
            return None
        hi = df_htf["high"].astype(float).values
        lo = df_htf["low"].astype(float).values
        cl = df_htf["close"].astype(float).values
        atr_h = calculate_atr(df_htf)
        if not atr_h or atr_h <= 0:
            return None
        last = cl[-1]
        best_lvl, best_d = None, float("inf")
        for off in range(4, 81):
            i = n - 1 - off
            if i - 3 < 0:
                break
            if direction == "BEARISH":
                lvl = lo[i]
                if lvl > lo[i - 3:i + 4].min():
                    continue
                if not (cl[i + 1:] < lvl).any():
                    continue
                if lvl > last and (lvl - last) < best_d:
                    best_d, best_lvl = lvl - last, lvl
            else:
                lvl = hi[i]
                if lvl < hi[i - 3:i + 4].max():
                    continue
                if not (cl[i + 1:] > lvl).any():
                    continue
                if lvl < last and (last - lvl) < best_d:
                    best_d, best_lvl = last - lvl, lvl
        if best_lvl is None:
            return None
        if direction == "BEARISH":
            return best_lvl - RZ_BACK_ATR * atr_h, best_lvl + RZ_UP_ATR * atr_h
        return best_lvl - RZ_UP_ATR * atr_h, best_lvl + RZ_BACK_ATR * atr_h
    except Exception:
        return None


def _rz_entry_sl_tp(direction: str, price: float, z_lo: float, z_hi: float, atr: float) -> tuple:
    """Entry/SL/TP untuk entry di zona retest HTF: entry = harga dijepit ke zona, SL di luar zona + buffer."""
    entry = min(max(price, z_lo), z_hi)
    buf = RZ_SL_BUF_ATR * (atr or 0.0)
    if direction == "BULLISH":
        sl = z_lo - buf
        dist = entry - sl
        tp1 = entry + dist * TP1_R_MULT
        tp2 = entry + dist * (RR_GRADE_A + 0.5)
    else:
        sl = z_hi + buf
        dist = sl - entry
        tp1 = entry - dist * TP1_R_MULT
        tp2 = entry - dist * (RR_GRADE_A + 0.5)
    if dist <= 0:
        return entry, sl, tp1, tp2, 0.0, 0.0
    if SINGLE_TP_MODE:
        tp1 = entry + dist * SINGLE_TP_R_MULT if direction == "BULLISH" else entry - dist * SINGLE_TP_R_MULT
        tp2 = tp1
    rr1 = round(abs(tp1 - entry) / dist, 2)
    rr2 = round(abs(tp2 - entry) / dist, 2)
    return entry, sl, tp1, tp2, rr1, rr2


def _ltf_structure_ok(df: pd.DataFrame, direction: str) -> tuple:
    """
    Filter struktur LTF: (ok: bool, alasan: str).
    BULLISH butuh HH + HL dan belum CHoCH (tidak ada close di bawah HL terakhir); BEARISH kebalikannya.
    """
    try:
        highs, lows = find_swings(df)
    except Exception:
        return True, "swing error (filter diabaikan)"

    def _dedupe(sw):
        # swing berdekatan (high/low sama di candle bersebelahan) dihitung satu
        out = []
        for it in sw:
            if not out or it[0] - out[-1][0] > SWING_WINDOW:
                out.append(it)
        return out

    highs, lows = _dedupe(highs), _dedupe(lows)
    if len(highs) < 2 or len(lows) < 2:
        return False, "swing kurang (<2 high/low)"
    closes = df["close"]
    if direction == "BULLISH":
        hh = highs[-1][1] > highs[-2][1]
        hl = lows[-1][1] > lows[-2][1]
        if not (hh and hl):
            return False, f"bukan HH+HL (HH={hh}, HL={hl})"
        after = closes.iloc[lows[-1][0] + 1:]
        if len(after) and float(after.min()) < lows[-1][1]:
            return False, "CHoCH: close di bawah HL terakhir"
        return True, "HH+HL terjaga"
    lh = highs[-1][1] < highs[-2][1]
    ll = lows[-1][1] < lows[-2][1]
    if not (lh and ll):
        return False, f"bukan LH+LL (LH={lh}, LL={ll})"
    after = closes.iloc[highs[-1][0] + 1:]
    if len(after) and float(after.max()) > highs[-1][1]:
        return False, "CHoCH: close di atas LH terakhir"
    return True, "LH+LL terjaga"


# ══════════════════════════════════════════════════════════════════════════
# VALIDASI BREAKOUT/BREAKDOWN DENGAN CANDLE ENGULFING (anti-fakeout)
# ──────────────────────────────────────────────────────────────────────────
# Mode:
#   "TAG"   → TIDAK mengubah keputusan trade; hanya MENCATAT engulfing valid/tidak ke trade log
#             (untuk mengukur dengan data: apakah trade ber-engulfing memang lebih sering menang?)
#   "GATE"  → tanpa engulfing valid, sinyal DITOLAK (paling ketat)
#   "SCORE" → tanpa engulfing valid, BOS diturunkan STRONG → WEAK (skor turun)
#   "OFF"   → fitur dimatikan (perilaku lama)
# ══════════════════════════════════════════════════════════════════════════
ENGULF_BREAK_MODE        = "TAG"
ENGULF_BREAK_LOOKBACK    = 24    # cari candle tembus di N candle TERTUTUP terakhir (pullback butuh waktu)
ENGULF_BODY_MIN          = 0.55  # body candle tembus >= 55% dari panjang candle
ENGULF_CLOSE_ZONE        = 0.30  # close harus di 30% paling atas (bull) / bawah (bear)
ENGULF_TOLERANCE         = 0.0005  # toleransi "menelan" (0.05%) krn open crypto ~ close sebelumnya
ENGULF_REQUIRE_HOLD      = True  # setelah tembus, tidak boleh ada close yang menghapus seluruh candle engulfing (lewat OPEN-nya)
ENGULF_ALLOW_MOMENTUM    = False # True = candle sebelumnya searah pun boleh asal body >= 1.5x & body 70%+
ENGULF_REQUIRE_FVG       = True  # candle engulfing harus meninggalkan FVG (bukti dorongan nyata)
ENGULF_FVG_MIN_ATR       = 0.25  # ukuran FVG minimal = 0.25 x ATR (di bawah itu dianggap kecil)
ENGULF_FVG_MAX_FILL      = 1.00  # 1.0 = tidak dipakai. Pullback ke OB memang MENGISI FVG, jadi jangan ditolak karenanya

# Konfirmasi saat MASUK: candle tertutup terakhir harus engulfing searah trade
ENTRY_ENGULF_MODE        = "OFF"    # "GATE" = wajib | "OFF" = mati  (nyalakan SETELAH filter lain terbukti menghasilkan trade)
ENTRY_ENGULF_BODY_MIN    = 0.50     # body >= 50% dari panjang candle
ENTRY_ENGULF_CLOSE_ZONE  = 0.35     # close di 35% teratas (long) / terbawah (short)

# Retest FVG: setelah breakout (BOS) + FVG besar, harga balik masuk FVG (retest).
# Valid kalau candle retest CLOSE di sisi searah dari TENGAH FVG (atas tengah utk LONG,
# bawah tengah utk SHORT) dan tidak pernah close menembus FVG sampai ujung seberang.
FVG_RETEST_MODE          = "TAG"    # "TAG" = hanya catat | "GATE" = wajib | "OFF" = mati

# Konfirmasi STOCHASTIC pada pair (candle TERTUTUP, TF entry), dua bukti terpisah:
#   stoch_div  = divergence searah (SHORT: harga HH tapi %K LH | LONG: harga LL tapi %K HL)
#   stoch_weak = pelemahan momentum (SHORT: %K sempat >= 80 dlm STOCH_LOOKBACK candle,
#                lalu %K di bawah %D dan turun | LONG: kebalikannya dari zona <= 20)
# Mode:
#   "TAG"      = hanya catat ke trade log (default, tidak menolak sinyal)
#   "GATE_ANY" = wajib salah satu (div ATAU weak) pada arah di STOCH_GATE_DIRECTIONS
#   "GATE_DIV" = wajib ada divergence pada arah di STOCH_GATE_DIRECTIONS
#   "OFF"      = mati
# CATATAN: STOCH_POSITION_GATE_ENABLED (/togglestochgate) tidak mengendalikan ini;
# di versi sebelumnya flag itu tidak dicek di analyze_pair().
STOCH_CONFIRM_MODE       = "TAG"
STOCH_GATE_DIRECTIONS    = ("BEARISH",)   # gate hanya untuk SHORT; tag tetap dicatat utk semua arah


def detect_engulfing_break(df: pd.DataFrame, direction: str) -> tuple:
    """
    Cek apakah breakout (BULLISH) / breakdown (BEARISH) benar-benar ditembus
    oleh candle ENGULFING yang kuat, dan sudah bertahan (tidak balik masuk).
    HANYA memakai candle yang sudah TUTUP (candle terakhir yang masih berjalan dibuang).
    Returns: (ok: bool, alasan: str)
    """
    try:
        if df is None or len(df) < 40:
            return False, "data kurang"
        d = df.iloc[:-1].reset_index(drop=True)          # buang candle yang masih berjalan
        n = len(d)
        highs, lows = find_swings(d)
        swings = highs if direction == "BULLISH" else lows
        if not swings:
            return False, "tidak ada swing level"

        o = d["open"].astype(float).values;  h = d["high"].astype(float).values
        l = d["low"].astype(float).values;   c = d["close"].astype(float).values

        # cari candle tembus TERBARU dalam lookback: close pertama yang melewati swing terakhir
        brk_i, level = None, None
        for i in range(n - 1, max(n - 1 - ENGULF_BREAK_LOOKBACK, 1), -1):
            # level penting = swing paling ekstrem dalam 40 candle sebelum candle i
            prior = [p for (idx, p) in swings if i - 40 <= idx < i]
            if not prior:
                continue
            lv = max(prior) if direction == "BULLISH" else min(prior)
            if direction == "BULLISH":
                crossed = c[i] > lv and c[i - 1] <= lv
            else:
                crossed = c[i] < lv and c[i - 1] >= lv
            if crossed:
                brk_i, level = i, lv
                break
        if brk_i is None:
            return False, "belum ada candle yang menembus level swing"

        i, rng = brk_i, h[brk_i] - l[brk_i]
        if rng <= 0:
            return False, "candle tembus tanpa range"
        body = abs(c[i] - o[i]); body_pct = body / rng
        pbody = abs(c[i - 1] - o[i - 1])
        ago = n - 1 - i

        if body_pct < ENGULF_BODY_MIN:
            return False, f"candle tembus body kecil ({body_pct*100:.0f}% < {ENGULF_BODY_MIN*100:.0f}%)"

        tol = ENGULF_TOLERANCE
        if direction == "BULLISH":
            if c[i] <= o[i]:
                return False, "candle tembus bukan bullish"
            if (c[i] - l[i]) / rng < (1 - ENGULF_CLOSE_ZONE):
                return False, "close tidak dekat high (ada ekor atas panjang)"
            prev_opp = c[i - 1] < o[i - 1]
            engulf = prev_opp and o[i] <= c[i - 1] * (1 + tol) and c[i] >= o[i - 1] * (1 - tol)
        else:
            if c[i] >= o[i]:
                return False, "candle tembus bukan bearish"
            if (h[i] - c[i]) / rng < (1 - ENGULF_CLOSE_ZONE):
                return False, "close tidak dekat low (ada ekor bawah panjang)"
            prev_opp = c[i - 1] > o[i - 1]
            engulf = prev_opp and o[i] >= c[i - 1] * (1 - tol) and c[i] <= o[i - 1] * (1 + tol)

        if not engulf and ENGULF_ALLOW_MOMENTUM:
            engulf = body >= 1.5 * max(pbody, 1e-12) and body_pct >= 0.70
        if not engulf:
            return False, "candle tembus bukan engulfing (tidak menelan candle sebelumnya)"

        if ENGULF_REQUIRE_HOLD and ago > 0:
            # Retest/pullback ke level yang ditembus itu NORMAL (bahkan jadi tempat entry).
            # Fakeout baru terbukti kalau harga close melewati OPEN candle engulfing,
            # artinya seluruh candle engulfing sudah dihapus balik.
            after = c[i:]
            held = bool((after > o[i]).all()) if direction == "BULLISH" else bool((after < o[i]).all())
            if not held:
                return False, "seluruh candle engulfing dihapus balik (fakeout)"

        fvg_txt = ""
        if ENGULF_REQUIRE_FVG:
            if ago < 1:
                return False, "tunggu 1 candle lagi untuk memastikan ada FVG"
            # FVG = celah antara candle sebelum engulfing dan candle sesudahnya
            if direction == "BULLISH":
                g_bot, g_top = h[i - 1], l[i + 1]
            else:
                g_bot, g_top = h[i + 1], l[i - 1]
            gap = g_top - g_bot
            if gap <= 0:
                return False, "engulfing tanpa FVG (tidak ada celah dorongan)"
            tr = [max(h[k] - l[k], abs(h[k] - c[k - 1]), abs(l[k] - c[k - 1]))
                  for k in range(max(1, n - 14), n)]
            atr_loc = sum(tr) / len(tr) if tr else 0.0
            if atr_loc > 0 and gap < ENGULF_FVG_MIN_ATR * atr_loc:
                return False, f"FVG kecil ({gap/atr_loc:.2f}x ATR < {ENGULF_FVG_MIN_ATR}x)"
            later = slice(i + 2, n)
            if direction == "BULLISH":
                fill = (g_top - min(l[later])) / gap if n > i + 2 else 0.0
            else:
                fill = (max(h[later]) - g_bot) / gap if n > i + 2 else 0.0
            fill = max(0.0, min(1.0, fill))
            if fill > ENGULF_FVG_MAX_FILL:
                return False, f"FVG sudah terisi {fill*100:.0f}% (dorongan dimakan balik)"
            fvg_txt = f" | FVG {gap/atr_loc:.2f}x ATR, terisi {fill*100:.0f}%" if atr_loc > 0 else ""

        tag = "Bullish" if direction == "BULLISH" else "Bearish"
        return True, (f"{tag} Engulfing menembus {level:.6g} | body {body_pct*100:.0f}% | "
                      f"{ago} candle lalu, bertahan{fvg_txt}")
    except Exception as e:
        return False, f"error validasi engulfing: {e}"


def detect_entry_engulfing(df: pd.DataFrame, direction: str) -> tuple:
    """
    Konfirmasi saat masuk: candle TERTUTUP terakhir harus engulfing searah trade
    (bullish engulfing untuk LONG, bearish engulfing untuk SHORT).
    Returns: (ok: bool, alasan: str)
    """
    try:
        if df is None or len(df) < 5:
            return False, "data kurang"
        d = df.iloc[:-1]                       # buang candle yang masih berjalan
        o1, h1, l1, c1 = (float(d["open"].iloc[-1]), float(d["high"].iloc[-1]),
                          float(d["low"].iloc[-1]),  float(d["close"].iloc[-1]))
        o0, c0 = float(d["open"].iloc[-2]), float(d["close"].iloc[-2])
        rng = h1 - l1
        if rng <= 0:
            return False, "candle tanpa range"
        body_pct = abs(c1 - o1) / rng
        if body_pct < ENTRY_ENGULF_BODY_MIN:
            return False, f"candle konfirmasi body kecil ({body_pct*100:.0f}%)"
        tol = ENGULF_TOLERANCE
        if direction == "BULLISH":
            ok = (c0 < o0 and c1 > o1 and o1 <= c0 * (1 + tol) and c1 >= o0 * (1 - tol)
                  and (c1 - l1) / rng >= (1 - ENTRY_ENGULF_CLOSE_ZONE))
        else:
            ok = (c0 > o0 and c1 < o1 and o1 >= c0 * (1 - tol) and c1 <= o0 * (1 + tol)
                  and (h1 - c1) / rng >= (1 - ENTRY_ENGULF_CLOSE_ZONE))
        if not ok:
            return False, "candle terakhir bukan engulfing searah trade"
        return True, f"Konfirmasi entry: engulfing {'bullish' if direction == 'BULLISH' else 'bearish'} (body {body_pct*100:.0f}%)"
    except Exception as e:
        return False, f"error konfirmasi entry: {e}"


def detect_fvg_retest(df: pd.DataFrame, direction: str) -> tuple:
    """
    Setup: BOS (close menembus swing ekstrem) -> candle breakout kuat meninggalkan FVG
    besar -> harga RETEST masuk FVG -> candle retest close di sisi searah dari TENGAH FVG.
    Hanya candle yang sudah TUTUP yang dipakai.
    Returns: (ok: bool, alasan: str)
    """
    try:
        if df is None or len(df) < 40:
            return False, "data kurang"
        d = df.iloc[:-1].reset_index(drop=True)
        n = len(d)
        highs, lows = find_swings(d)
        swings = highs if direction == "BULLISH" else lows
        if not swings:
            return False, "tidak ada swing level"
        o = d["open"].astype(float).values;  h = d["high"].astype(float).values
        l = d["low"].astype(float).values;   c = d["close"].astype(float).values

        # candle breakout terbaru; harus ada >=1 candle SETELAH candle FVG untuk retest
        brk_i = None
        for i in range(n - 3, max(n - 1 - ENGULF_BREAK_LOOKBACK, 1), -1):
            prior = [p for (idx, p) in swings if i - 40 <= idx < i]
            if not prior:
                continue
            lv = max(prior) if direction == "BULLISH" else min(prior)
            crossed = (c[i] > lv and c[i - 1] <= lv) if direction == "BULLISH" else (c[i] < lv and c[i - 1] >= lv)
            if crossed:
                brk_i = i
                break
        if brk_i is None:
            return False, "belum ada breakout (BOS) yang valid"

        i = brk_i
        rng = h[i] - l[i]
        if rng <= 0 or abs(c[i] - o[i]) / rng < ENGULF_BODY_MIN:
            return False, "candle breakout body kecil"
        if (direction == "BULLISH" and c[i] <= o[i]) or (direction == "BEARISH" and c[i] >= o[i]):
            return False, "candle breakout salah arah"

        if direction == "BULLISH":
            g_bot, g_top = h[i - 1], l[i + 1]
        else:
            g_bot, g_top = h[i + 1], l[i - 1]
        gap = g_top - g_bot
        if gap <= 0:
            return False, "breakout tanpa FVG"
        tr = [max(h[k] - l[k], abs(h[k] - c[k - 1]), abs(l[k] - c[k - 1])) for k in range(max(1, n - 14), n)]
        atr_loc = sum(tr) / len(tr) if tr else 0.0
        if atr_loc > 0 and gap < ENGULF_FVG_MIN_ATR * atr_loc:
            return False, f"FVG kecil ({gap/atr_loc:.2f}x ATR)"
        ce = (g_bot + g_top) / 2.0

        after = range(i + 2, n)
        if direction == "BULLISH":
            if any(c[k] < g_bot for k in after):
                return False, "harga sudah close menembus FVG ke bawah"
            entered = [k for k in after if l[k] <= g_top]
        else:
            if any(c[k] > g_top for k in after):
                return False, "harga sudah close menembus FVG ke atas"
            entered = [k for k in after if h[k] >= g_bot]
        if not entered:
            return False, "belum retest FVG"
        k = entered[-1]
        if direction == "BULLISH" and c[k] < ce:
            return False, "retest close di bawah tengah FVG"
        if direction == "BEARISH" and c[k] > ce:
            return False, "retest close di atas tengah FVG"
        side = "atas" if direction == "BULLISH" else "bawah"
        return True, f"Retest FVG {g_bot:.6g}-{g_top:.6g}, close {c[k]:.6g} di {side} tengah ({ce:.6g})"
    except Exception as e:
        return False, f"error retest FVG: {e}"


def detect_structure(df: pd.DataFrame) -> tuple:
    """
    Returns: (trend, event, last_swing_high, last_swing_low)
    ALIGNED WITH BASE: dikembalikan ke versi flat persis signal-only —
    EMA50 tiebreaker & 3-swing dominance check (v30/v30b) dihapus.
    CHoCH (lh+hl atau hh+ll) langsung dianggap RANGING, tidak dicoba
    "diselamatkan" jadi trending lewat konteks tambahan.
    """
    highs, lows = find_swings(df)
    if len(highs) < 2 or len(lows) < 2:
        return "RANGING", None, None, None

    last_sh, prev_sh = highs[-1][1], highs[-2][1]
    last_sl, prev_sl = lows[-1][1],  lows[-2][1]

    hh = last_sh > prev_sh
    hl = last_sl > prev_sl
    lh = last_sh < prev_sh
    ll = last_sl < prev_sl

    if hh and hl:   return "BULLISH", "BOS",   last_sh, last_sl
    if lh and ll:   return "BEARISH", "BOS",   last_sh, last_sl
    if hh and ll:   return "RANGING", "CHoCH", last_sh, last_sl
    if lh and hl:   return "RANGING", "CHoCH", last_sh, last_sl
    return "RANGING", None, None, None




def _ema_bias_fallback(df: pd.DataFrame) -> tuple:
    """
    Fallback bias menggunakan EMA50 dan EMA200 saat swing structure tidak jelas.
    Return: (trend, event, None, None)
    - Price < EMA50 < EMA200  → BEARISH (strong bearish)
    - Price < EMA50           → BEARISH
    - Price > EMA50 > EMA200  → BULLISH (strong bullish)
    - Price > EMA50           → BULLISH
    - Lainnya                 → RANGING

    FIX v42: Tambah buffer ±0.15% sebelum return BULLISH/BEARISH agar tidak
    flip-flop di market choppy saat price oscillate dekat EMA50.
    """
    try:
        if df is None or len(df) < 10:
            return "RANGING", None, None, None
        closes = df["close"].astype(float)
        price  = float(closes.iloc[-1])
        ema50  = float(closes.ewm(span=50, adjust=False).mean().iloc[-1])
        ema200 = float(closes.ewm(span=200, adjust=False).mean().iloc[-1]) if len(closes) >= 50 else ema50

        _buf = ema50 * 0.0015   # buffer 0.15% — cegah flip-flop di zona konsolidasi EMA50

        if price < ema50 - _buf:
            return "BEARISH", "EMA_BIAS", None, None
        elif price > ema50 + _buf:
            return "BULLISH", "EMA_BIAS", None, None
        else:
            return "RANGING", None, None, None
    except Exception:
        return "RANGING", None, None, None


def get_ref_bias(df_ref: Optional[pd.DataFrame]) -> str:
    if df_ref is None:
        return "N/A"
    trend, _, _, _ = detect_structure(df_ref)
    return trend



def get_btcd_bias() -> str:
    """
    1:1 DENGAN BASE (signal-only main46): btcd_trend = get_btcd_trend() dari
    BTCDOMUSDT di BTCDOM_HTF_TF (1h), limit=60, nilai MENTAH 'RISING' |
    'FALLING' | 'FLAT'.

    BUG FIX: versi lama memetakan ke 'BULLISH'/'BEARISH', padahal
    macro_score() mencocokkan 'RISING'/'FALLING' -> macro_pts SELALU 0
    ("Macro neutral") di autotrade, sedangkan base dapat +8 / -8 / -4.
    Juga sebelumnya pakai TF 15m (BTCD_TF), base pakai 1h.
    """
    try:
        df_btcd = fetch_btcd_ohlcv(tf=BTCDOM_HTF_TF, limit=60)
        return get_btcd_trend(df_btcd)   # sudah return FLAT jika data kurang
    except Exception as e:
        print(f"  ⚠️  get_btcd_bias error: {e}")
        return "FLAT"


# ═══════════════════════════════════════════════════════════════════════════
# ██  MARKET REGIME DETECTOR — BTC + BTCD EMA Real-Time
# ═══════════════════════════════════════════════════════════════════════════
#
# Logika sederhana & tegas:
#   BULL_REGIME  : BTC EMA bullish + BTCD turun  → LONG ok, SHORT DIBLOK
#   BEAR_REGIME  : BTC EMA bearish + BTCD naik   → SHORT ok, LONG DIBLOK
#   NEUTRAL      : Kondisi lain                   → kedua arah boleh (ikut sinyal biasa)
#
# Deteksi pakai EMA (bukan swing) agar responsif terhadap crash/pump mendadak.
# Cache 3 menit agar tidak overload API di setiap scan pair.
# ═══════════════════════════════════════════════════════════════════════════

_regime_cache: dict = {"regime": "NEUTRAL", "reason": "", "ts": 0.0}
_REGIME_CACHE_TTL = 180   # detik — refresh setiap 3 menit

# ── ALIGNED WITH BASE: regime versi signal-only (sample % altcoin trending) ──
# Ini beda pendekatan dari get_market_regime() di atas (yang pakai EMA BTC/BTCD
# real-time). BASE menyample sejumlah altcoin likuid, hitung berapa % yang
# sedang BULLISH/BEARISH via detect_structure(), lalu wajib dikonfirmasi BTC
# bias — baru dianggap BULL_REGIME/BEAR_REGIME. Dipakai untuk check_regime_block()
# di bawah, yang di BASE MEMANG hard-block sinyal berlawanan arah (bukan cuma
# penyesuaian skor).
REGIME_SAMPLE_PAIRS = [
    "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT",
    "AVAX/USDT", "LINK/USDT", "DOT/USDT", "NEAR/USDT", "APT/USDT",
]
REGIME_TF              = "1h"
REGIME_BULL_THRESHOLD  = 0.70
REGIME_BEAR_THRESHOLD  = 0.70
REGIME_CACHE_TTL       = 120

_market_regime: dict = {
    "regime": "MIXED", "bull_pct": 0.0, "bear_pct": 0.0,
    "sample_size": 0, "btc_aligned": True, "ts": 0.0, "detail": "",
}
_regime_lock = threading.Lock()
_regime_compute_lock = threading.Lock()   # v78: cuma 1 thread yang hitung ulang regime saat cache habis


def compute_market_regime(btc_bias: str) -> dict:
    """ALIGNED WITH BASE — porting verbatim. Sample altcoin, hitung % bull/bear, wajib align BTC."""
    bull_count = 0
    bear_count = 0
    sampled    = 0
    for pair in REGIME_SAMPLE_PAIRS:
        try:
            df = fetch_ohlcv_safe(pair, REGIME_TF, limit=100)
            bias, _, _, _ = detect_structure(df)
            if bias == "BULLISH":
                bull_count += 1
            elif bias == "BEARISH":
                bear_count += 1
            sampled += 1
            time.sleep(0.05)
        except Exception as e:
            print(f"  ⚠️  Regime sample {pair}: {e}")

    if sampled == 0:
        return {"regime": "MIXED", "bull_pct": 0.0, "bear_pct": 0.0,
                "sample_size": 0, "btc_aligned": True, "detail": "Data tidak tersedia"}

    bull_pct = bull_count / sampled
    bear_pct = bear_count / sampled

    raw_regime = "MIXED"
    if bull_pct >= REGIME_BULL_THRESHOLD:
        raw_regime = "BULL_REGIME"
    elif bear_pct >= REGIME_BEAR_THRESHOLD:
        raw_regime = "BEAR_REGIME"

    btc_aligned = True
    if raw_regime == "BULL_REGIME" and btc_bias != "BULLISH":
        btc_aligned = False
        raw_regime  = "MIXED"
    elif raw_regime == "BEAR_REGIME" and btc_bias != "BEARISH":
        btc_aligned = False
        raw_regime  = "MIXED"

    bull_em = "🟢" if raw_regime == "BULL_REGIME" else ("🟡" if bull_pct >= 0.50 else "⚪")
    bear_em = "🔴" if raw_regime == "BEAR_REGIME" else ("🟡" if bear_pct >= 0.50 else "⚪")
    btc_em  = "✅" if btc_aligned else "⚠️ BTC tdk konfirmasi"
    detail  = (
        f"{bull_em} Bullish: {bull_count}/{sampled} ({bull_pct*100:.0f}%) | "
        f"{bear_em} Bearish: {bear_count}/{sampled} ({bear_pct*100:.0f}%) | "
        f"BTC:{btc_bias} {btc_em} → {raw_regime}"
    )
    return {"regime": raw_regime, "bull_pct": round(bull_pct, 3), "bear_pct": round(bear_pct, 3),
            "sample_size": sampled, "btc_aligned": btc_aligned, "detail": detail}


def get_market_regime_signalonly(btc_bias: str, force_refresh: bool = False) -> dict:
    """ALIGNED WITH BASE — cache thread-safe, dipakai bareng check_regime_block()."""
    global _market_regime
    now = time.time()
    with _regime_lock:
        age = now - _market_regime["ts"]
        if not force_refresh and age < REGIME_CACHE_TTL and _market_regime["ts"] > 0:
            return _market_regime

    with _regime_compute_lock:
        # v78: cek ulang cache — thread lain mungkin sudah selesai menghitung saat kita menunggu lock
        with _regime_lock:
            if (not force_refresh and _market_regime["ts"] > 0
                    and (time.time() - _market_regime["ts"]) < REGIME_CACHE_TTL):
                return _market_regime
        result = compute_market_regime(btc_bias)
        result["ts"] = time.time()
        with _regime_lock:
            _market_regime = result

    regime_em = {"BULL_REGIME": "🟢", "BEAR_REGIME": "🔴", "MIXED": "⚪"}.get(result["regime"], "⚪")
    print(f"  🌐 Market Regime (SIGNALONLY): {regime_em} {result['regime']} | {result['detail']}")
    return result


def check_regime_block(direction: str, regime: dict, pair: str) -> tuple:
    """
    ALIGNED WITH BASE — hard block: LONG diblok total saat BEAR_REGIME,
    SHORT diblok total saat BULL_REGIME. BTC/ETH dikecualikan (ALTCOIN_EXEMPTIONS).
    Ini gate yang SEBELUMNYA dihapus/dianggap dead code di NEWBOT — di BASE
    gate ini memang aktif dan berjalan.
    """
    if pair in ALTCOIN_EXEMPTIONS:
        return False, ""
    r = regime.get("regime", "MIXED")
    if direction == "BULLISH" and r == "BEAR_REGIME":
        pct = regime.get("bear_pct", 0) * 100
        return True, f"🚫 BEAR REGIME ({pct:.0f}% alt turun + BTC {regime.get('btc_aligned','')}) — LONG diblok"
    if direction == "BEARISH" and r == "BULL_REGIME":
        pct = regime.get("bull_pct", 0) * 100
        return True, f"🚫 BULL REGIME ({pct:.0f}% alt naik + BTC {regime.get('btc_aligned','')}) — SHORT diblok"
    return False, ""


def get_market_regime() -> tuple:
    """
    Deteksi market regime dari BTC EMA + BTCD EMA secara real-time.

    Return: (regime: str, reason: str, block_long: bool, block_short: bool)
      regime  : 'BULL_REGIME' | 'BEAR_REGIME' | 'NEUTRAL'
      reason  : penjelasan singkat untuk log
      block_long  : True jika semua sinyal LONG harus diblok
      block_short : True jika semua sinyal SHORT harus diblok
    """
    global _regime_cache
    now = time.time()

    # Return cache jika masih valid
    if (now - _regime_cache["ts"]) < _REGIME_CACHE_TTL:
        r = _regime_cache
        return r["regime"], r["reason"], r["block_long"], r["block_short"]

    regime     = "NEUTRAL"
    reason     = "BTC/BTCD kondisi campuran — kedua arah diizinkan"
    block_long  = False
    block_short = False

    try:
        # ── Fetch BTC LTF (1h, 60 candle) untuk EMA direction ────────────────
        df_btc = fetch_ohlcv_safe("BTC/USDT", "1h", limit=60)
        if df_btc is None or len(df_btc) < 30:
            raise ValueError("BTC data tidak cukup")

        closes_btc = df_btc["close"].astype(float)
        btc_ema9   = float(closes_btc.ewm(span=9,  adjust=False).mean().iloc[-1])
        btc_ema21  = float(closes_btc.ewm(span=21, adjust=False).mean().iloc[-1])
        btc_price  = float(closes_btc.iloc[-1])

        # BTC bullish: harga di atas EMA9 dan EMA9 di atas EMA21
        btc_bullish = (btc_price > btc_ema9) and (btc_ema9 > btc_ema21)
        # BTC bearish: harga di bawah EMA9 dan EMA9 di bawah EMA21
        btc_bearish = (btc_price < btc_ema9) and (btc_ema9 < btc_ema21)

        # ── Fetch BTCD LTF untuk EMA direction ───────────────────────────────
        df_btcd = fetch_btcd_ohlcv_isolated(tf=BTCDOM_LTF_TF, limit=60)
        btcd_rising  = False
        btcd_falling = False

        if df_btcd is not None and len(df_btcd) >= 20:
            closes_btcd  = df_btcd["close"].astype(float)
            btcd_ema9    = float(closes_btcd.ewm(span=9,  adjust=False).mean().iloc[-1])
            btcd_ema21   = float(closes_btcd.ewm(span=21, adjust=False).mean().iloc[-1])
            btcd_price   = float(closes_btcd.iloc[-1])
            btcd_rising  = (btcd_price > btcd_ema9)  and (btcd_ema9  > btcd_ema21)
            btcd_falling = (btcd_price < btcd_ema9)  and (btcd_ema9  < btcd_ema21)

        # ── Tentukan regime ───────────────────────────────────────────────────
        # v27: regime tidak blok sinyal — jadi konteks + penalty/bonus score saja.
        # BTC stabil + DOM turun (alt season) → tetap izinkan LONG meski BTC flat.
        # BTC drop + DOM naik → penalty untuk LONG, tapi tidak diblok total.
        if btc_bullish and btcd_falling:
            # BTC naik + Dominance turun = Alt season → kondisi terbaik untuk LONG alt
            regime      = "BULL_REGIME"
            block_short = False   # v27: tidak blok SHORT — bisa jadi alt individual shortable
            block_long  = False
            reason      = (
                f"🟢 BULL REGIME: BTC EMA bullish (price={btc_price:.0f} > EMA9={btc_ema9:.0f} > EMA21={btc_ema21:.0f}) "
                f"+ BTCD EMA turun → Alt season → LONG alt IDEAL (SHORT dapat penalty score)"
            )

        elif btc_bearish and btcd_rising:
            # BTC turun + Dominance naik = Alt bleeding → kondisi terbaik untuk SHORT alt
            regime     = "BEAR_REGIME"
            block_long  = False   # v27: tidak blok LONG — alt season bisa terjadi kapanpun
            block_short = False
            reason     = (
                f"🔴 BEAR REGIME: BTC EMA bearish (price={btc_price:.0f} < EMA9={btc_ema9:.0f} < EMA21={btc_ema21:.0f}) "
                f"+ BTCD EMA naik → Alt bleeding → SHORT alt IDEAL (LONG dapat penalty score)"
            )

        else:
            # Kondisi campuran atau transisi → neutral, biarkan sinyal PA/SMC yang bicara
            btc_state  = "BULLISH" if btc_bullish else ("BEARISH" if btc_bearish else "MIXED")
            btcd_state = "RISING"  if btcd_rising  else ("FALLING" if btcd_falling  else "MIXED")
            reason     = (
                f"⚪ NEUTRAL: BTC EMA={btc_state}, BTCD EMA={btcd_state} "
                f"— tidak ada kondisi dominan, semua arah diizinkan"
            )

    except Exception as e:
        reason = f"⚠️ Regime error: {e} — fallback NEUTRAL"
        print(f"  ⚠️  get_market_regime: {e}")

    # Simpan ke cache
    _regime_cache = {
        "regime":      regime,
        "reason":      reason,
        "block_long":  block_long,
        "block_short": block_short,
        "ts":          now,
    }
    return regime, reason, block_long, block_short


# ── Signal Hash — deduplikasi sinyal yang sama ───────────────────────────────
# ── Signal Hash — deduplikasi sinyal yang sama (ALIGNED WITH BASE) ──────────
_sent_signal_hashes: dict = {}   # key: md5 hash_str → sent_at (datetime, insertion time)

def _make_signal_hash(pair, direction, entry, sl) -> str:
    """Buat hash unik dari kombinasi pair + direction + entry (rounded) + sl (rounded).
    Selalu normalisasi ke LONG/SHORT agar konsisten antara BULLISH/BEARISH
    (trade_direction) dan LONG/SHORT (yang disimpan di JSON posisi).
    Dibulatkan ke 2 desimal agar entry yang bergerak tipis tidak dianggap sinyal baru.
    """
    dir_norm = direction if direction in ("LONG", "SHORT") \
               else ("LONG" if direction == "BULLISH" else "SHORT")
    raw = f"{pair}|{dir_norm}|{round(entry, 2)}|{round(sl, 2)}"
    return hashlib.md5(raw.encode()).hexdigest()

def _is_duplicate_signal(pair, direction, entry, sl) -> bool:
    """
    Return True jika sinyal dengan kombinasi pair+direction+entry+sl yang sama
    sudah pernah dikirim dalam SIGNAL_HASH_TTL_HOURS jam terakhir.
    """
    now     = datetime.now(timezone.utc)
    expired = [h for h, ts in _sent_signal_hashes.items()
               if (now - ts).total_seconds() / 3600 >= SIGNAL_HASH_TTL_HOURS]
    for h in expired:
        _sent_signal_hashes.pop(h, None)
    return _make_signal_hash(pair, direction, entry, sl) in _sent_signal_hashes

def _register_signal_hash(pair, direction, entry, sl):
    """Daftarkan sinyal ke hash registry — akan expired setelah SIGNAL_HASH_TTL_HOURS."""
    _sent_signal_hashes[_make_signal_hash(pair, direction, entry, sl)] = datetime.now(timezone.utc)




# ── _tg_send: alias untuk send_telegram_raw dengan optional chat_id ──────────
def _tg_send(msg: str, chat_id: str = None):
    """
    Kirim pesan Telegram. Alias dari send_telegram_raw.
    chat_id opsional — jika None, pakai TELEGRAM_CHAT_ID default.
    """
    if chat_id and chat_id != TELEGRAM_CHAT_ID:
        # Kirim ke chat_id spesifik
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try:
            requests.post(url, data={
                "chat_id": chat_id, "text": msg, "parse_mode": "HTML"
            }, timeout=10)
        except Exception as e:
            print(f"⚠️ _tg_send error: {e}")
    else:
        send_telegram_raw(msg)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 6 — LIQUIDITY SWEEP
# ═══════════════════════════════════════════════════════════════════════════

def detect_eqh_eql(df: pd.DataFrame) -> dict:
    recent   = df.iloc[-SWEEP_LOOKBACK:]
    h_vals   = recent["high"].values
    l_vals   = recent["low"].values
    eqh_pool, eql_pool = [], []

    for i in range(len(h_vals)):
        for j in range(i + 1, len(h_vals)):
            if abs(h_vals[i] - h_vals[j]) / (h_vals[i] + 1e-9) <= EQH_EQL_TOLERANCE:
                eqh_pool.append(max(h_vals[i], h_vals[j]))
    for i in range(len(l_vals)):
        for j in range(i + 1, len(l_vals)):
            if abs(l_vals[i] - l_vals[j]) / (l_vals[i] + 1e-9) <= EQH_EQL_TOLERANCE:
                eql_pool.append(min(l_vals[i], l_vals[j]))

    return {
        "eqh": float(max(eqh_pool)) if eqh_pool else None,
        "eql": float(min(eql_pool)) if eql_pool else None,
    }


def detect_liquidity_sweep(df: pd.DataFrame, trend: str, strict: bool = True) -> tuple:
    recent     = df.iloc[-SWEEP_LOOKBACK:]
    c          = df.iloc[-1]
    c_prev     = df.iloc[-2]
    eq         = detect_eqh_eql(df)
    swing_high = float(recent["high"].max())
    swing_low  = float(recent["low"].min())

    def wick_body_ratio(candle, direction: str) -> float:
        o, h, l, cl = (float(candle[x]) for x in ["open", "high", "low", "close"])
        body = abs(cl - o)
        if body < 1e-9:
            return 0.0
        return (min(o, cl) - l) / body if direction == "BULLISH" else (h - max(o, cl)) / body

    def close_rejected(candle, level: float, direction: str) -> bool:
        cl = float(candle["close"])
        return cl > level * (1 - REJECTION_TOLERANCE) if direction == "BULLISH" \
            else cl < level * (1 + REJECTION_TOLERANCE)

    if trend == "BULLISH":
        eql = eq.get("eql")
        for candle in [c_prev, c]:
            swept_level = None
            if float(candle["low"]) < swing_low:
                swept_level = swing_low
            elif eql and float(candle["low"]) < eql:
                swept_level = eql
            if swept_level is not None:
                rejected = close_rejected(candle, swept_level, "BULLISH")
                ratio    = wick_body_ratio(candle, "BULLISH")
                if strict:
                    if rejected and ratio >= WICK_BODY_RATIO_MIN:
                        return True, "Bullish Sweep + Strong Rejection", swept_level, "STRONG"
                else:
                    if rejected:
                        strength = "STRONG" if ratio >= WICK_BODY_RATIO_MIN else "RELAXED"
                        return True, f"Bullish Sweep + Rejection ({strength})", swept_level, strength

    elif trend == "BEARISH":
        eqh = eq.get("eqh")
        for candle in [c_prev, c]:
            swept_level = None
            if float(candle["high"]) > swing_high:
                swept_level = swing_high
            elif eqh and float(candle["high"]) > eqh:
                swept_level = eqh
            if swept_level is not None:
                rejected = close_rejected(candle, swept_level, "BEARISH")
                ratio    = wick_body_ratio(candle, "BEARISH")
                if strict:
                    if rejected and ratio >= WICK_BODY_RATIO_MIN:
                        return True, "Bearish Sweep + Strong Rejection", swept_level, "STRONG"
                else:
                    if rejected:
                        strength = "STRONG" if ratio >= WICK_BODY_RATIO_MIN else "RELAXED"
                        return True, f"Bearish Sweep + Rejection ({strength})", swept_level, strength

    return False, None, None, None


def find_liquidity_target_ahead(df: pd.DataFrame, entry: float, tp_rr: float, direction: str) -> Optional[float]:
    """
    Cari liquidity level (swing high/low dari SWEEP_LOOKBACK candle terakhir,
    atau equal-high/equal-low dari detect_eqh_eql — SUMBER SAMA dengan yang
    dipakai detect_liquidity_sweep untuk validasi entry) yang ada DI ANTARA
    entry dan tp_rr (TP hasil perhitungan RR murni).

    Kalau ketemu, return level itu (BELUM dikurangi buffer — pemanggil yang
    menerapkan LIQUIDITY_TP_BUFFER_PCT, supaya konstanta buffer tetap satu
    tempat saja di analyze_pair). Kalau tidak ada level relevan, return None
    (pemanggil pakai tp_rr apa adanya sebagai fallback).

    LONG: cari level TERTINGGI yang < tp_rr dan > entry (swing high / eqh
    yang paling dekat dari bawah ke arah TP — itu yang paling mungkin
    "menarik" harga buat disapu duluan sebelum lanjut/reversal).
    SHORT: sebaliknya, cari level TERENDAH yang > tp_rr dan < entry.
    """
    recent     = df.iloc[-SWEEP_LOOKBACK:]
    eq         = detect_eqh_eql(df)
    swing_high = float(recent["high"].max())
    swing_low  = float(recent["low"].min())

    candidates = []
    if direction == "LONG":
        for lvl in [swing_high, eq.get("eqh")]:
            if lvl is not None and entry < lvl < tp_rr:
                candidates.append(lvl)
        if not candidates:
            return None
        return min(candidates)  # level PALING DEKAT dari entry (paling mungkin ditarget duluan)
    else:
        for lvl in [swing_low, eq.get("eql")]:
            if lvl is not None and tp_rr < lvl < entry:
                candidates.append(lvl)
        if not candidates:
            return None
        return max(candidates)  # level PALING DEKAT dari entry


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 7 — ORDER BLOCK
# ═══════════════════════════════════════════════════════════════════════════

def find_order_blocks(df: pd.DataFrame, trend: str) -> list:
    """1:1 PORT dari base signal-only (main41) — sort by strength saja."""
    ob_list = []
    start   = max(1, len(df) - OB_LOOKBACK)
    price   = float(df["close"].iloc[-1])

    for i in range(start, len(df) - 2):
        c   = df.iloc[i]
        nxt = df.iloc[i + 1]
        impulse_pct = abs(float(nxt["close"]) - float(nxt["open"])) / (float(nxt["open"]) + 1e-9) * 100

        if impulse_pct < MIN_DISPLACEMENT_PCT:
            continue

        is_valid = False
        if trend == "BULLISH" and float(c["close"]) < float(c["open"]) and float(nxt["close"]) > float(nxt["open"]):
            is_valid = True
        elif trend == "BEARISH" and float(c["close"]) > float(c["open"]) and float(nxt["close"]) < float(nxt["open"]):
            is_valid = True

        if not is_valid:
            continue

        ob   = {"low": float(c["low"]), "high": float(c["high"]),
                "mid": (float(c["low"]) + float(c["high"])) / 2,
                "index": i, "impulse": impulse_pct}
        taps = sum(
            1 for j in range(i + 2, len(df))
            if float(df.iloc[j]["low"]) <= ob["high"] and float(df.iloc[j]["high"]) >= ob["low"]
        )
        if taps >= OB_MITIGATION_LIMIT:
            continue

        ob["taps"]     = taps
        dist_pct       = abs(price - ob["mid"]) / (price + 1e-9) * 100
        ob["strength"] = impulse_pct / (1.0 + dist_pct) / (1.0 + taps)
        ob_list.append(ob)

    ob_list.sort(key=lambda x: x["strength"], reverse=True)
    return ob_list[:3]


def find_opposing_ob_in_path(
    df: pd.DataFrame,
    direction: str,
    entry: float,
    tp_candidate: float,
    atr: float,
) -> Optional[dict]:
    """
    Cari OB berlawanan arah yang ada DI ANTARA entry dan tp_candidate.

    Untuk LONG  → cari OB BEARISH (supply) antara entry dan tp_candidate.
    Untuk SHORT → cari OB BULLISH (demand) antara entry dan tp_candidate.

    Hanya OB yang mid-nya berada di dalam window (entry, tp_candidate) yang dihitung.
    OB terlalu dekat entry (< 0.3×ATR) diabaikan agar tidak terganggu noise.

    Return: OB yang paling dekat ke entry (hambatan pertama yang harus dilewati), atau None.
    """
    opposing_dir = "BEARISH" if direction == "BULLISH" else "BULLISH"
    ob_opposing  = find_order_blocks(df, opposing_dir)
    if not ob_opposing:
        return None

    blocking_obs = []
    for ob in ob_opposing:
        ob_mid = ob["mid"]
        if direction == "BULLISH":
            # OB supply harus berada di antara entry dan tp_candidate
            if (entry + atr * 0.3) < ob_mid < tp_candidate:
                blocking_obs.append(ob)
        else:  # SHORT
            # OB demand harus berada di antara tp_candidate dan entry
            if tp_candidate < ob_mid < (entry - atr * 0.3):
                blocking_obs.append(ob)

    if not blocking_obs:
        return None

    # Ambil OB paling dekat ke entry (hambatan pertama yang harus ditembus dulu)
    if direction == "BULLISH":
        blocking_obs.sort(key=lambda x: x["mid"])        # ascending → paling dekat entry dulu
    else:
        blocking_obs.sort(key=lambda x: x["mid"], reverse=True)  # descending

    return blocking_obs[0]


def price_in_ob(price: float, ob_list: list, tolerance: float = 0.004) -> tuple:
    if not ob_list:
        return False, None
    lo   = price * (1 - tolerance)
    hi   = price * (1 + tolerance)
    hits = [ob for ob in ob_list if ob["low"] <= hi and ob["high"] >= lo]
    return (True, max(hits, key=lambda x: x["strength"])) if hits else (False, None)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 8 — FAIR VALUE GAP
# ═══════════════════════════════════════════════════════════════════════════

def find_fvg(df: pd.DataFrame, trend: str) -> Optional[dict]:
    start    = max(1, len(df) - FVG_LOOKBACK)
    price    = float(df["close"].iloc[-1])
    fvg_list = []

    for i in range(start, len(df) - 1):
        p1 = df.iloc[i - 1]
        p3 = df.iloc[i + 1]

        if trend == "BULLISH":
            gap     = float(p3["low"]) - float(p1["high"])
            min_gap = float(p1["high"]) * MIN_FVG_PCT / 100
            if gap < min_gap:
                continue
            fvg_top, fvg_bot = float(p3["low"]), float(p1["high"])
        elif trend == "BEARISH":
            gap     = float(p1["low"]) - float(p3["high"])
            min_gap = float(p3["high"]) * MIN_FVG_PCT / 100
            if gap < min_gap:
                continue
            fvg_top, fvg_bot = float(p1["low"]), float(p3["high"])
        else:
            continue

        post     = df.iloc[i + 2:]
        fill_pct = 0.0
        if len(post) > 0:
            gap_size = fvg_top - fvg_bot
            if trend == "BULLISH":
                deepest  = float(post["low"].min())
                fill_pct = min(1.0, max(0.0, (fvg_top - deepest) / gap_size)) if gap_size > 0 else 0.0
            else:
                deepest  = float(post["high"].max())
                fill_pct = min(1.0, max(0.0, (deepest - fvg_bot) / gap_size)) if gap_size > 0 else 0.0

        if fill_pct >= 0.85:
            continue

        fvg_mid = (fvg_top + fvg_bot) / 2
        in_fvg  = fvg_bot * (1 - 0.005) <= price <= fvg_top * (1 + 0.005)
        fvg_list.append({"top": fvg_top, "bottom": fvg_bot, "mid": fvg_mid,
                         "fill_pct": fill_pct, "index": i, "in_zone": in_fvg})

    if not fvg_list:
        return None
    in_zone = [f for f in fvg_list if f["in_zone"]]
    return in_zone[-1] if in_zone else fvg_list[-1]


def price_in_fvg(price: float, fvg: Optional[dict]) -> bool:
    if not fvg:
        return False
    return fvg["bottom"] * 0.995 <= price <= fvg["top"] * 1.005


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 8b — BODY IMBALANCE (IMB) & PRICE GAP
# ═══════════════════════════════════════════════════════════════════════════

def find_imb(df: pd.DataFrame, trend: str) -> Optional[dict]:
    """
    Deteksi Body Imbalance (IMB) — zona antara body candle impulsif.

    IMB BULLISH: zona antara close candle sebelumnya dan open candle impulsif bullish
    IMB BEARISH: zona antara close candle sebelumnya dan open candle impulsif bearish

    Berbeda dari FVG (gap wick-to-wick), IMB fokus pada body candle sehingga
    lebih konservatif dan sering lebih akurat sebagai zona entry/support.
    """
    start    = max(2, len(df) - IMB_LOOKBACK)
    price    = float(df["close"].iloc[-1])
    imb_list = []

    for i in range(start, len(df) - 1):
        c    = df.iloc[i]
        prev = df.iloc[i - 1]
        o, h, l, cl  = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        candle_range = h - l
        if candle_range < 1e-9:
            continue
        body_ratio = abs(cl - o) / candle_range

        # Harus candle impulsif (body besar)
        if body_ratio < IMB_MIN_BODY_PCT:
            continue

        prev_cl = float(prev["close"])

        if trend == "BULLISH" and cl > o:
            # IMB Bullish: zona antara prev_close dan open candle bullish ini
            imb_top = o
            imb_bot = prev_cl
            if imb_top <= imb_bot:
                continue
        elif trend == "BEARISH" and cl < o:
            # IMB Bearish: zona antara open candle bearish dan prev_close
            imb_top = prev_cl
            imb_bot = o
            if imb_top <= imb_bot:
                continue
        else:
            continue

        # Ukuran minimal
        size_pct = (imb_top - imb_bot) / (imb_bot + 1e-9) * 100
        if size_pct < IMB_MIN_SIZE_PCT:
            continue

        # Cek fill: seberapa dalam harga sudah masuk ke zona IMB setelah terbentuk
        post     = df.iloc[i + 1:]
        fill_pct = 0.0
        gap_size = imb_top - imb_bot
        if len(post) > 0 and gap_size > 0:
            if trend == "BULLISH":
                deepest  = float(post["low"].min())
                fill_pct = min(1.0, max(0.0, (imb_top - deepest) / gap_size))
            else:
                deepest  = float(post["high"].max())
                fill_pct = min(1.0, max(0.0, (deepest - imb_bot) / gap_size))

        if fill_pct >= IMB_FILL_LIMIT:
            continue  # IMB sudah terisi terlalu dalam → tidak valid lagi

        imb_mid = (imb_top + imb_bot) / 2
        in_zone = imb_bot * (1 - 0.005) <= price <= imb_top * (1 + 0.005)
        imb_list.append({
            "top":      imb_top, "bottom": imb_bot, "mid": imb_mid,
            "fill_pct": fill_pct, "index": i, "in_zone": in_zone,
            "size_pct": size_pct,
        })

    if not imb_list:
        return None
    in_zone = [z for z in imb_list if z["in_zone"]]
    return in_zone[-1] if in_zone else imb_list[-1]


def price_in_imb(price: float, imb: Optional[dict]) -> bool:
    if not imb:
        return False
    return imb["bottom"] * 0.995 <= price <= imb["top"] * 1.005


def find_gap(df: pd.DataFrame, trend: str) -> Optional[dict]:
    """
    Deteksi Price Gap — area kosong antara high/low dua candle yang tidak overlap.

    Gap BULLISH: low candle ke-3 > high candle ke-1 (harga loncat naik, ada void di bawah)
    Gap BEARISH: high candle ke-3 < low candle ke-1 (harga loncat turun, ada void di atas)
    """
    start    = max(1, len(df) - GAP_LOOKBACK)
    price    = float(df["close"].iloc[-1])
    gap_list = []

    for i in range(start, len(df) - 1):
        c1 = df.iloc[i - 1]
        c3 = df.iloc[i + 1]
        c1_high = float(c1["high"])
        c1_low  = float(c1["low"])
        c3_high = float(c3["high"])
        c3_low  = float(c3["low"])

        if trend == "BULLISH":
            # Gap bullish: c3_low > c1_high → ada ruang di bawah (magnet saat pullback)
            gap_size = c3_low - c1_high
            if gap_size <= 0:
                continue
            gap_top = c3_low
            gap_bot = c1_high
        elif trend == "BEARISH":
            # Gap bearish: c3_high < c1_low → ada ruang di atas (magnet saat pullback)
            gap_size = c1_low - c3_high
            if gap_size <= 0:
                continue
            gap_top = c1_low
            gap_bot = c3_high
        else:
            continue

        # Ukuran minimal (cegah noise micro-gap)
        ref_price = (gap_top + gap_bot) / 2
        size_pct  = gap_size / (ref_price + 1e-9) * 100
        if size_pct < GAP_MIN_SIZE_PCT:
            continue

        # Fill check: cek apakah harga sudah menutup gap
        post     = df.iloc[i + 2:]
        fill_pct = 0.0
        if len(post) > 0 and gap_size > 0:
            if trend == "BULLISH":
                deepest  = float(post["low"].min())
                fill_pct = min(1.0, max(0.0, (gap_top - deepest) / gap_size))
            else:
                deepest  = float(post["high"].max())
                fill_pct = min(1.0, max(0.0, (deepest - gap_bot) / gap_size))

        if fill_pct >= 0.90:
            continue  # Gap sudah 90% tertutup → tidak relevan

        gap_mid = (gap_top + gap_bot) / 2
        in_zone = gap_bot * (1 - 0.005) <= price <= gap_top * (1 + 0.005)
        gap_list.append({
            "top":      gap_top, "bottom": gap_bot, "mid": gap_mid,
            "fill_pct": fill_pct, "index": i, "in_zone": in_zone,
            "size_pct": size_pct,
        })

    if not gap_list:
        return None
    in_zone = [g for g in gap_list if g["in_zone"]]
    return in_zone[-1] if in_zone else gap_list[-1]


def price_in_gap(price: float, gap: Optional[dict]) -> bool:
    if not gap:
        return False
    return gap["bottom"] * 0.995 <= price <= gap["top"] * 1.005


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 9 — DISPLACEMENT & VOLUME
# ═══════════════════════════════════════════════════════════════════════════

def detect_displacement(df: pd.DataFrame, trend: str) -> bool:
    threshold = MIN_DISPLACEMENT_PCT * 2
    for candle in [df.iloc[-1], df.iloc[-2]]:
        o, cl    = float(candle["open"]), float(candle["close"])
        body_pct = abs(cl - o) / (o + 1e-9) * 100
        if body_pct >= threshold:
            if trend == "BULLISH" and cl > o:
                return True
            if trend == "BEARISH" and cl < o:
                return True
    return False


def volume_ratio(df: pd.DataFrame, lookback: int = 20) -> float:
    avg = df["volume"].iloc[-lookback:-1].mean()
    vol = float(df["volume"].iloc[-1])
    return round(vol / avg, 2) if avg > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 10 — RSI
# ═══════════════════════════════════════════════════════════════════════════

def calculate_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> float:
    closes = df["close"].values
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_g  = sum(gains[:period]) / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    return round(100.0 - (100.0 / (1.0 + avg_g / avg_l)), 2)


def calculate_rsi_series(df: pd.DataFrame, period: int = RSI_PERIOD) -> list:
    """
    Sama seperti calculate_rsi() tapi mengembalikan RSI di TIAP candle
    (bukan cuma nilai terakhir) — dipakai RSI_POSITION_GATE untuk cek
    "pernah overbought/oversold dalam N candle terakhir" + cross-back
    melalui zona reversal, sama seperti k_series/d_series punya
    calculate_stochastic() sebelumnya.
    """
    closes = df["close"].values
    n = len(closes)
    if n < period + 1:
        return [50.0] * n
    deltas = [closes[i] - closes[i - 1] for i in range(1, n)]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]
    avg_g  = sum(gains[:period]) / period
    avg_l  = sum(losses[:period]) / period
    rsi_series = [50.0] * period  # padding untuk index 0..period (belum ada data cukup)
    rsi_series.append(100.0 if avg_l == 0 else round(100.0 - (100.0 / (1.0 + avg_g / avg_l)), 2))
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rsi_series.append(100.0 if avg_l == 0 else round(100.0 - (100.0 / (1.0 + avg_g / avg_l)), 2))
    # rsi_series sekarang punya panjang == n (1 nilai per candle, termasuk padding awal)
    return rsi_series[:n] if len(rsi_series) >= n else rsi_series + [rsi_series[-1]] * (n - len(rsi_series))


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 10b — MACD (display-only, tidak dipakai untuk scoring/gate)
# ═══════════════════════════════════════════════════════════════════════════
#
# Ditambahkan supaya signal card Telegram punya "Technical Snapshot" (RSI,
# MACD, resistance terdekat) mirip alat riset seperti Neurobro — murni
# tambahan informasi untuk conviction visual saat entry. TIDAK dipakai di
# manapun sebagai gate/filter/scoring (analyze_pair() logic tidak berubah).

def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal_period: int = 9) -> dict:
    """
    MACD standar (EMA12 - EMA26, signal = EMA9 dari MACD line).
    Return dict siap pakai untuk display:
      {macd, signal, histogram, trend, crossover}
    - trend: "BULLISH" kalau MACD line > signal line, else "BEARISH"
    - crossover: "BULLISH_CROSS" / "BEARISH_CROSS" kalau baru cross di candle
      terakhir, else None
    """
    closes = df["close"]
    if len(closes) < slow + signal_period:
        return {"macd": 0.0, "signal": 0.0, "histogram": 0.0,
                "trend": "NEUTRAL", "crossover": None}

    ema_fast = closes.ewm(span=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    hist_series = macd_line - signal_line

    macd_now,  macd_prev  = float(macd_line.iloc[-1]),   float(macd_line.iloc[-2])
    sig_now,   sig_prev   = float(signal_line.iloc[-1]), float(signal_line.iloc[-2])
    hist_now              = float(hist_series.iloc[-1])

    trend = "BULLISH" if macd_now > sig_now else "BEARISH"

    crossover = None
    if macd_prev <= sig_prev and macd_now > sig_now:
        crossover = "BULLISH_CROSS"
    elif macd_prev >= sig_prev and macd_now < sig_now:
        crossover = "BEARISH_CROSS"

    return {
        "macd":      round(macd_now, 6),
        "signal":    round(sig_now, 6),
        "histogram": round(hist_now, 6),
        "trend":     trend,
        "crossover": crossover,
    }


def get_nearest_levels(df: pd.DataFrame, current_price: float) -> dict:
    """
    Resistance & support terdekat dari harga sekarang, pakai swing highs/lows
    yang sudah dihitung find_swings() (dipakai juga oleh detect_structure()).
    Display-only — bukan sumber SL/TP (itu tetap dari OB/FVG/ATR seperti biasa).
      {resistance, support}  (None kalau tidak ada swing di sisi itu)
    """
    highs, lows = find_swings(df)

    res_candidates = [p for _, p in highs if p > current_price]
    sup_candidates = [p for _, p in lows if p < current_price]

    resistance = min(res_candidates) if res_candidates else None
    support    = max(sup_candidates) if sup_candidates else None

    return {"resistance": resistance, "support": support}


def rsi_score(rsi: float, direction: str = "") -> int:
    """
    1:1 PORT dari base signal-only (main41).
    FIX (2026-08-31): dibalik berdasarkan backtest 444 trade — RSI ekstrem
    (>75 / <25) WR 55% (+19.7R, n=20) jauh lebih baik daripada RSI "ideal"
    50-65 yang justru WR 26.6% (-35.5R, n=218). Skor sebelumnya kebalik.
    Parameter `direction` tetap diterima agar call site lama (compute_score)
    tidak perlu diubah, tapi tidak dipakai untuk mempengaruhi hasil.
    """
    if rsi > 75 or rsi < 25:
        return SCORE_RSI_IDEAL
    if 50 <= rsi <= 65:
        return SCORE_RSI_PENALTY
    return 0


def regime_score(adx: float, rsi: float, momentum_aligned: bool) -> int:
    """
    SOFT-SCORE regime ADX (2026-09-15) — lihat komentar di ADX_RANGING_THRESHOLD
    untuk desain lengkap. Tidak pernah reject/block, cuma nambah/kurangi poin.
    """
    rsi_extreme = rsi > 75 or rsi < 25
    if adx < ADX_RANGING_THRESHOLD:
        if rsi_extreme:
            return SCORE_ADX_RANGING_RSI_BONUS
        if momentum_aligned:
            return SCORE_ADX_RANGING_MOMENTUM_PENALTY
        return 0
    if adx >= ADX_TRENDING_MIN and momentum_aligned:
        return SCORE_ADX_TRENDING_MOMENTUM_BONUS
    return 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 11 — MACRO
# ═══════════════════════════════════════════════════════════════════════════

# 1:1 DENGAN main73: cache 1 slot {"df","ts"} (lihat fetch_btcd_ohlcv).
_btcd_cache: dict = {"df": None, "ts": 0.0}

# v15: Ticker BTCD di Binance Futures
BTCDOM_TICKER    = "BTCDOMUSDT"
BTCDOM_HTF_TF    = "1h"    # disamakan dengan bot signal-only
BTCDOM_LTF_TF    = "15m"   # LTF untuk korelasi real-time
BTCD_CACHE_TTL   = 60      # 1:1 dgn base signal-only (60 detik)


def fetch_btcd_ohlcv(tf: str = BTCDOM_LTF_TF, limit: int = 80) -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV BTCDOMUSDT dari Binance Futures.
    Fallback ke CoinGecko jika ticker tidak tersedia.
    Return: DataFrame OHLCV atau None

    1:1 DENGAN main73: cache 1 slot (df, ts) tanpa key TF/limit — persis
    perilaku main73, termasuk quirk-nya (panggilan 15m/80 dan 1h/60 dalam
    jendela BTCD_CACHE_TTL berbagi slot yang sama). Caller SISTEM (snapshot
    eksekusi, /status, regime info) TIDAK boleh pakai fungsi ini — pakai
    fetch_btcd_ohlcv_isolated() supaya tidak menimpa slot cache strategi.
    """
    global _btcd_cache
    now = time.time()
    # Gunakan cache jika masih fresh (keduanya pakai tf yang sama)
    if (
        _btcd_cache["df"] is not None
        and (now - _btcd_cache["ts"]) < BTCD_CACHE_TTL
    ):
        return _btcd_cache["df"]

    # Coba Binance Futures BTCDOMUSDT
    try:
        df = fetch_ohlcv(BTCDOM_TICKER, tf, limit=limit)
        _btcd_cache = {"df": df, "ts": now}
        return df
    except Exception as e:
        print(f"  ⚠️  BTCDOMUSDT fetch error ({tf}): {e} — fallback CoinGecko")

    # Fallback: CoinGecko (data harian, akurasi LTF rendah)
    try:
        url    = "https://api.coingecko.com/api/v3/global/market_cap_chart?vs_currency=usd&days=7"
        r      = requests.get(url, timeout=10, headers={"User-Agent": "SMC-Bot"})
        data   = r.json()
        pts    = data.get("market_cap_percentage", {}).get("btc", [])
        if len(pts) >= 5:
            # Bangun DataFrame minimal dari data CoinGecko
            rows = [{"time": p[0], "open": p[1], "high": p[1],
                     "low": p[1], "close": p[1], "volume": 0} for p in pts]
            df_cg = pd.DataFrame(rows)
            df_cg["time"] = pd.to_datetime(df_cg["time"], unit="ms")
            _btcd_cache = {"df": df_cg, "ts": now}
            return df_cg
    except Exception as e2:
        print(f"  ⚠️  BTCD CoinGecko fallback error: {e2}")

    return None


# Cache TERPISAH (key = (tf, limit)) khusus caller sistem/info — tidak
# menyentuh _btcd_cache milik jalur strategi.
_btcd_cache_isolated: dict = {}


def fetch_btcd_ohlcv_isolated(tf: str = BTCDOM_LTF_TF, limit: int = 80) -> Optional[pd.DataFrame]:
    """Versi lama (cache per-(tf,limit)) untuk caller non-strategi."""
    now = time.time()
    _ck = (tf, limit)
    _hit = _btcd_cache_isolated.get(_ck)
    if _hit is not None and _hit["df"] is not None and (now - _hit["ts"]) < BTCD_CACHE_TTL:
        return _hit["df"]
    try:
        df = fetch_ohlcv_safe(BTCDOM_TICKER, tf, limit=limit)
        if df is None:
            raise RuntimeError("no data")
        _btcd_cache_isolated[_ck] = {"df": df, "ts": now}
        return df
    except Exception as e:
        print(f"  ⚠️  BTCDOMUSDT fetch error ({tf}): {e} — fallback CoinGecko")
    try:
        url    = "https://api.coingecko.com/api/v3/global/market_cap_chart?vs_currency=usd&days=7"
        r      = requests.get(url, timeout=10, headers={"User-Agent": "SMC-Bot"})
        data   = r.json()
        pts    = data.get("market_cap_percentage", {}).get("btc", [])
        if len(pts) >= 5:
            rows = [{"time": p[0], "open": p[1], "high": p[1],
                     "low": p[1], "close": p[1], "volume": 0} for p in pts]
            df_cg = pd.DataFrame(rows)
            df_cg["time"] = pd.to_datetime(df_cg["time"], unit="ms")
            _btcd_cache_isolated[_ck] = {"df": df_cg, "ts": now}
            return df_cg
    except Exception as e2:
        print(f"  ⚠️  BTCD CoinGecko fallback error: {e2}")
    return None


def fetch_btc_dominance_series() -> Optional[pd.DataFrame]:
    """Wrapper — return DataFrame BTCDOM (bukan list CoinGecko seperti versi lama)."""
    return fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)


def get_btcd_trend(df_btcd: Optional[pd.DataFrame]) -> str:
    """
    Deteksi trend BTCD HTF dari DataFrame OHLCV BTCDOMUSDT.
    Gunakan EMA perbandingan: EMA20 vs EMA50 di close.
    Return: 'RISING', 'FALLING', atau 'FLAT'
    """
    if df_btcd is None or len(df_btcd) < BTCD_SMA_PERIOD * 2:
        return "FLAT"
    closes   = df_btcd["close"].values
    sma_now  = float(closes[-BTCD_SMA_PERIOD:].mean())
    sma_prev = float(closes[-BTCD_SMA_PERIOD * 2:-BTCD_SMA_PERIOD].mean())
    diff_pct = (sma_now - sma_prev) / (sma_prev + 1e-9) * 100
    if diff_pct > 0.3:  return "RISING"
    if diff_pct < -0.3: return "FALLING"
    return "FLAT"


# ═══════════════════════════════════════════════════════════════════════════
# ██  STOCHASTIC RSI ENGINE — Setting 5,3,3 (Digunakan untuk BTC Multi-TF)
# ═══════════════════════════════════════════════════════════════════════════

def calc_stoch_rsi(df: pd.DataFrame, rsi_len: int = 5, stoch_len: int = 5,
                   k_smooth: int = 3, d_smooth: int = 3) -> tuple:
    """
    Hitung Stochastic RSI dengan setting 5,3,3.
    Return: (k_line: float, d_line: float)
      k < 20   = Oversold zone
      k > 80   = Overbought zone
    """
    if df is None or len(df) < rsi_len + stoch_len + k_smooth + d_smooth + 5:
        return 50.0, 50.0

    closes = df["close"].astype(float)

    # ── Hitung RSI ─────────────────────────────────────────────────────────────
    delta  = closes.diff()
    gain   = delta.clip(lower=0)
    loss   = -delta.clip(upper=0)
    avg_g  = gain.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    avg_l  = loss.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    rs     = avg_g / avg_l.replace(0, 1e-9)
    rsi    = 100 - (100 / (1 + rs))

    # ── Hitung Stochastic dari RSI ─────────────────────────────────────────────
    rsi_low  = rsi.rolling(stoch_len).min()
    rsi_high = rsi.rolling(stoch_len).max()
    raw_k    = 100 * (rsi - rsi_low) / (rsi_high - rsi_low + 1e-9)

    # Smoothing K dan D
    k_line   = raw_k.rolling(k_smooth).mean()
    d_line   = k_line.rolling(d_smooth).mean()

    k_val = float(k_line.iloc[-1]) if not k_line.empty else 50.0
    d_val = float(d_line.iloc[-1]) if not d_line.empty else 50.0

    if pd.isna(k_val): k_val = 50.0
    if pd.isna(d_val): d_val = 50.0

    return round(k_val, 2), round(d_val, 2)


def get_stoch_rsi_state(k: float, d: float) -> str:
    """
    Terjemahkan nilai Stochastic RSI menjadi state kondisi pasar.
    Return: 'OVERBOUGHT' | 'OVERSOLD' | 'NEUTRAL' | 'CROSSING_UP' | 'CROSSING_DOWN'
    """
    if k > 80 and d > 80:
        return "OVERBOUGHT"
    if k < 20 and d < 20:
        return "OVERSOLD"
    if k > 80 and k > d and d <= 80:
        return "OVERBOUGHT"
    if k < 20 and k < d and d >= 20:
        return "OVERSOLD"
    # Crossing patterns
    if k > d and k < 50:
        return "CROSSING_UP"
    if k < d and k > 50:
        return "CROSSING_DOWN"
    return "NEUTRAL"


# ═══════════════════════════════════════════════════════════════════════════
# ██  DIVERGENCE DETECTOR — Bullish & Bearish untuk Konfirmasi Entry
# ═══════════════════════════════════════════════════════════════════════════

def detect_divergence(df: pd.DataFrame, rsi_len: int = 5, stoch_len: int = 5,
                       k_smooth: int = 3, d_smooth: int = 3,
                       lookback: int = 30) -> str:
    """
    Deteksi Bullish / Bearish Divergence menggunakan Stochastic RSI (5,3,3).

    Bullish Divergence: Harga buat Lower Low, Stoch RSI buat Higher Low
      → sinyal potensi reversal naik (bagus untuk konfirmasi LONG)

    Bearish Divergence: Harga buat Higher High, Stoch RSI buat Lower High
      → sinyal potensi reversal turun (bagus untuk konfirmasi SHORT)

    Return: 'BULLISH_DIV' | 'BEARISH_DIV' | 'NONE'
    """
    if df is None or len(df) < lookback + 10:
        return "NONE"

    closes = df["close"].astype(float)
    highs  = df["high"].astype(float)
    lows   = df["low"].astype(float)

    # Hitung Stoch RSI untuk seluruh window
    delta  = closes.diff()
    gain   = delta.clip(lower=0)
    loss   = -delta.clip(upper=0)
    avg_g  = gain.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    avg_l  = loss.ewm(com=rsi_len - 1, min_periods=rsi_len).mean()
    rs     = avg_g / avg_l.replace(0, 1e-9)
    rsi    = 100 - (100 / (1 + rs))

    rsi_low  = rsi.rolling(stoch_len).min()
    rsi_high = rsi.rolling(stoch_len).max()
    raw_k    = 100 * (rsi - rsi_low) / (rsi_high - rsi_low + 1e-9)
    k_line   = raw_k.rolling(k_smooth).mean()

    recent_df = df.iloc[-lookback:]
    recent_k  = k_line.iloc[-lookback:]

    # Cari swing lows (untuk bullish div) dan swing highs (untuk bearish div)
    price_lows  = []
    price_highs = []
    k_lows      = []
    k_highs     = []

    window_sw = 3   # kecil untuk LTF, cukup untuk swing pivot
    prices_l  = recent_df["low"].values
    prices_h  = recent_df["high"].values
    k_vals    = recent_k.values

    for i in range(window_sw, len(prices_l) - window_sw):
        # Swing low harga
        if prices_l[i] == min(prices_l[i - window_sw: i + window_sw + 1]):
            price_lows.append((i, prices_l[i]))
        # Swing high harga
        if prices_h[i] == max(prices_h[i - window_sw: i + window_sw + 1]):
            price_highs.append((i, prices_h[i]))
        # Swing low stoch RSI
        if not pd.isna(k_vals[i]):
            if k_vals[i] == min([v for v in k_vals[i - window_sw: i + window_sw + 1] if not pd.isna(v)] or [k_vals[i]]):
                k_lows.append((i, k_vals[i]))
            if k_vals[i] == max([v for v in k_vals[i - window_sw: i + window_sw + 1] if not pd.isna(v)] or [k_vals[i]]):
                k_highs.append((i, k_vals[i]))

    # ── Cek Bullish Divergence: Harga LL, Stoch RSI HL ───────────────────────
    if len(price_lows) >= 2 and len(k_lows) >= 2:
        # Bandingkan dua swing low terakhir
        pl1_i, pl1_p = price_lows[-2]
        pl2_i, pl2_p = price_lows[-1]
        # Cari k_low terdekat dengan setiap swing low harga
        kl1 = min(k_lows, key=lambda x: abs(x[0] - pl1_i), default=None)
        kl2 = min(k_lows, key=lambda x: abs(x[0] - pl2_i), default=None)
        if kl1 and kl2:
            price_ll = pl2_p < pl1_p    # harga: Lower Low
            stoch_hl = kl2[1] > kl1[1]  # stoch: Higher Low
            if price_ll and stoch_hl and kl1[1] < 40:   # konfirmasi: stoch dari zona rendah
                return "BULLISH_DIV"

    # ── Cek Bearish Divergence: Harga HH, Stoch RSI LH ──────────────────────
    if len(price_highs) >= 2 and len(k_highs) >= 2:
        ph1_i, ph1_p = price_highs[-2]
        ph2_i, ph2_p = price_highs[-1]
        kh1 = min(k_highs, key=lambda x: abs(x[0] - ph1_i), default=None)
        kh2 = min(k_highs, key=lambda x: abs(x[0] - ph2_i), default=None)
        if kh1 and kh2:
            price_hh = ph2_p > ph1_p    # harga: Higher High
            stoch_lh = kh2[1] < kh1[1]  # stoch: Lower High
            if price_hh and stoch_lh and kh1[1] > 60:   # konfirmasi: stoch dari zona tinggi
                return "BEARISH_DIV"

    return "NONE"


# ═══════════════════════════════════════════════════════════════════════════
# ██  BTC MULTI-TF ANALYSIS — H4 → H1 (Daily sebagai konteks)
#     Strategy: H4 menentukan bias utama (lebih responsif), Daily sebagai konfirmasi
#               H1 untuk timing entry
#
#     Jika BTC RANGING di H4 → tidak ada posisi sama sekali
#     Stochastic RSI 5,3,3 dipakai di setiap TF
#     Jika BTC RANGING di Daily → tidak ada posisi sama sekali
# ═══════════════════════════════════════════════════════════════════════════

# Cache hasil analisa BTC agar tidak re-fetch setiap pair
_btc_multitf_cache: dict = {"result": None, "ts": 0.0}
_BTC_MULTITF_CACHE_TTL = 300   # 5 menit cache — struktur BTC H4/Daily tidak berubah tiap 2 menit

@dataclass
class BtcMultiTfResult:
    """Hasil analisa BTC multi-timeframe."""
    bias: str           # 'BULLISH' | 'BEARISH' | 'RANGING'
    daily_bias: str     # bias dari Daily candle
    h4_bias: str        # bias dari H4 candle
    h1_bias: str        # bias dari H1 candle
    daily_stoch_k: float
    daily_stoch_d: float
    daily_stoch_state: str   # OVERBOUGHT / OVERSOLD / NEUTRAL / CROSSING_UP / CROSSING_DOWN
    h4_stoch_k: float
    h4_stoch_d: float
    h4_stoch_state: str
    h1_stoch_k: float
    h1_stoch_d: float
    h1_stoch_state: str
    h1_divergence: str   # BULLISH_DIV / BEARISH_DIV / NONE
    h4_divergence: str
    setup_valid: bool    # True = ada setup BTC yang jelas (bukan ranging)
    reason: str          # penjelasan singkat untuk log/Telegram
    allow_long: bool     # boleh ambil LONG alt
    allow_short: bool    # boleh ambil SHORT alt


def analyze_btc_multitf() -> BtcMultiTfResult:
    """
    Analisa BTC secara bertingkat: H4 → H1 (Daily sebagai konteks).

    LOGIKA HIERARKI:
    1. H4: Tentukan bias utama (BULLISH / BEARISH / RANGING)
       - H4 RANGING + Daily ada bias → pakai Daily
       - H4+Daily keduanya RANGING → setup_valid=False
    2. Daily: Digunakan sebagai konteks / konfirmasi
    3. H1: Cari entry timing + divergence
    """
    global _btc_multitf_cache

    now_ts = time.time()
    if (
        _btc_multitf_cache["result"] is not None
        and (now_ts - _btc_multitf_cache["ts"]) < _BTC_MULTITF_CACHE_TTL
    ):
        return _btc_multitf_cache["result"]

    def _fallback(reason: str) -> BtcMultiTfResult:
        return BtcMultiTfResult(
            bias="RANGING", daily_bias="RANGING", h4_bias="RANGING", h1_bias="RANGING",
            daily_stoch_k=50.0, daily_stoch_d=50.0, daily_stoch_state="NEUTRAL",
            h4_stoch_k=50.0, h4_stoch_d=50.0, h4_stoch_state="NEUTRAL",
            h1_stoch_k=50.0, h1_stoch_d=50.0, h1_stoch_state="NEUTRAL",
            h1_divergence="NONE", h4_divergence="NONE",
            setup_valid=False, reason=reason, allow_long=True, allow_short=True,
        )

    try:
        df_daily = fetch_ohlcv_safe("BTC/USDT", "1d",  limit=200)
        df_h4    = fetch_ohlcv_safe("BTC/USDT", "4h",  limit=200)
        df_h1    = fetch_ohlcv_safe("BTC/USDT", "1h",  limit=100)

        if df_daily is None or len(df_daily) < 50:
            return _fallback("BTC Daily data tidak cukup")
        if df_h4 is None or len(df_h4) < 50:
            return _fallback("BTC H4 data tidak cukup")

        daily_bias, _, _, _ = detect_structure(df_daily)
        d_k, d_d  = calc_stoch_rsi(df_daily)
        d_state   = get_stoch_rsi_state(d_k, d_d)

        h4_bias, _, _, _ = detect_structure(df_h4)
        h4_k, h4_d = calc_stoch_rsi(df_h4)
        h4_state   = get_stoch_rsi_state(h4_k, h4_d)
        h4_div     = detect_divergence(df_h4, lookback=40)

        h1_bias, _, _, _ = detect_structure(df_h1) if df_h1 is not None and len(df_h1) >= 30 else ("RANGING", None, None, None)
        h1_k, h1_d = calc_stoch_rsi(df_h1) if df_h1 is not None else (50.0, 50.0)
        h1_state   = get_stoch_rsi_state(h1_k, h1_d)
        h1_div     = detect_divergence(df_h1, lookback=30) if df_h1 is not None else "NONE"

        # H4 RANGING → fallback ke Daily
        if h4_bias == "RANGING":
            if daily_bias in ("BULLISH", "BEARISH"):
                result = BtcMultiTfResult(
                    bias=daily_bias,
                    daily_bias=daily_bias, h4_bias=h4_bias, h1_bias=h1_bias,
                    daily_stoch_k=d_k, daily_stoch_d=d_d, daily_stoch_state=d_state,
                    h4_stoch_k=h4_k, h4_stoch_d=h4_d, h4_stoch_state=h4_state,
                    h1_stoch_k=h1_k, h1_stoch_d=h1_d, h1_stoch_state=h1_state,
                    h1_divergence=h1_div, h4_divergence=h4_div,
                    setup_valid=True,
                    reason=(
                        f"⚠️ BTC H4 RANGING tapi Daily {daily_bias} → bias dari Daily. "
                        f"Stoch H4 K={h4_k:.1f}/D={h4_d:.1f} ({h4_state})."
                    ),
                    allow_long=True, allow_short=True,
                )
                _btc_multitf_cache = {"result": result, "ts": now_ts}
                return result
            else:
                result = BtcMultiTfResult(
                    bias="RANGING", daily_bias=daily_bias, h4_bias=h4_bias, h1_bias=h1_bias,
                    daily_stoch_k=d_k, daily_stoch_d=d_d, daily_stoch_state=d_state,
                    h4_stoch_k=h4_k, h4_stoch_d=h4_d, h4_stoch_state=h4_state,
                    h1_stoch_k=h1_k, h1_stoch_d=h1_d, h1_stoch_state=h1_state,
                    h1_divergence=h1_div, h4_divergence=h4_div,
                    setup_valid=False,
                    reason=(
                        f"🔀 BTC H4 RANGING + Daily RANGING — tidak ada arah jelas. "
                        f"Stoch H4 K={h4_k:.1f}/D={h4_d:.1f} ({h4_state})."
                    ),
                    allow_long=True, allow_short=True,
                )
                _btc_multitf_cache = {"result": result, "ts": now_ts}
                return result

        final_bias  = h4_bias
        reasons     = []

        if h4_bias == "BULLISH":
            reasons.append(f"✅ BTC H4 BULLISH → konteks positif untuk LONG alt")
            if daily_bias == "BULLISH":
                reasons.append(f"✅ Daily konfirmasi BULLISH (multi-TF aligned)")
            elif daily_bias == "RANGING":
                reasons.append(f"⚠️ Daily RANGING — H4 bullish tapi Daily belum trending")
            else:
                reasons.append(f"⚠️ Daily counter-BEARISH — konteks macro negatif")
            if h4_state == "OVERBOUGHT":
                reasons.append(f"⚠️ H4 Stoch OVERBOUGHT (K={h4_k:.1f}) — momentum bisa lemah")
            elif h4_state == "OVERSOLD":
                reasons.append(f"✅ H4 Stoch OVERSOLD (K={h4_k:.1f}) — momentum bullish fresh")
            if h1_div == "BULLISH_DIV":
                reasons.append(f"✅ H1 Bullish Divergence — timing entry LONG sangat baik")
            if h4_div == "BEARISH_DIV":
                reasons.append(f"⚠️ H4 Bearish Divergence — pertimbangkan tunda Long")
            if h1_state == "OVERSOLD":
                reasons.append(f"✅ H1 Stoch OVERSOLD (K={h1_k:.1f}) — pullback selesai")
        elif h4_bias == "BEARISH":
            reasons.append(f"✅ BTC H4 BEARISH → konteks negatif, SHORT alt relevan")
            if daily_bias == "BEARISH":
                reasons.append(f"✅ Daily konfirmasi BEARISH (multi-TF aligned)")
            elif daily_bias == "RANGING":
                reasons.append(f"⚠️ Daily RANGING — H4 bearish tapi Daily belum trending")
            else:
                reasons.append(f"⚠️ Daily counter-BULLISH — konteks macro positif")
            if h4_state == "OVERSOLD":
                reasons.append(f"⚠️ H4 Stoch OVERSOLD (K={h4_k:.1f}) — momentum bear bisa lemah")
            elif h4_state == "OVERBOUGHT":
                reasons.append(f"✅ H4 Stoch OVERBOUGHT (K={h4_k:.1f}) — momentum bearish fresh")
            if h1_div == "BEARISH_DIV":
                reasons.append(f"✅ H1 Bearish Divergence — timing entry SHORT sangat baik")
            if h4_div == "BULLISH_DIV":
                reasons.append(f"⚠️ H4 Bullish Divergence — pertimbangkan tunda Short")
            if h1_state == "OVERBOUGHT":
                reasons.append(f"✅ H1 Stoch OVERBOUGHT (K={h1_k:.1f}) — pullback selesai")

        # HTF Stoch Gate context (info saja di Multi-TF, gate hard di pair level)
        if HTF_STOCH_GATE_ENABLED:
            _h4_ob = h4_state in ("OVERBOUGHT", "CROSSING_DOWN") and h4_k > 60
            _h4_os = h4_state in ("OVERSOLD", "CROSSING_UP") and h4_k < 40
            if _h4_ob:
                reasons.append(
                    f"⚠️ BTC H4 Stoch {h4_state} (K={h4_k:.1f}) — BTC overbought, waspadai momentum melambat"
                )
            elif _h4_os:
                reasons.append(
                    f"⚠️ BTC H4 Stoch {h4_state} (K={h4_k:.1f}) — BTC oversold, waspadai tekanan jual berkurang"
                )

        result = BtcMultiTfResult(
            bias=final_bias, daily_bias=daily_bias, h4_bias=h4_bias, h1_bias=h1_bias,
            daily_stoch_k=d_k, daily_stoch_d=d_d, daily_stoch_state=d_state,
            h4_stoch_k=h4_k, h4_stoch_d=h4_d, h4_stoch_state=h4_state,
            h1_stoch_k=h1_k, h1_stoch_d=h1_d, h1_stoch_state=h1_state,
            h1_divergence=h1_div, h4_divergence=h4_div,
            setup_valid=True, reason=" | ".join(reasons),
            allow_long=True, allow_short=True,
        )
        _btc_multitf_cache = {"result": result, "ts": now_ts}
        return result

    except Exception as e:
        print(f"  ❌ analyze_btc_multitf error: {e}")
        return _fallback(f"Error analisa BTC: {e}")



def get_btc_bias() -> str:
    """
    ALIGNED WITH BASE (signal-only) — 1:1 dengan main.py get_btc_bias():
    single timeframe (BTC_BIAS_TF), detect_structure() sederhana. TIDAK
    lagi pakai analyze_btc_multitf() (Daily→H4→H1) karena itu menghasilkan
    btc_bias yang BEDA dari signal-only, sehingga macro_score() dan skor
    akhir sinyal ikut beda antara kedua bot meski analyze_pair() identik.
    analyze_btc_multitf() tetap dipakai TERPISAH untuk logging/context di
    main() — tapi tidak lagi jadi sumber btc_bias yang dilempar ke analyze_pair().
    """
    try:
        df_btc = fetch_ohlcv("BTC/USDT", BTC_BIAS_TF, limit=200)
        bias, _, _, _ = detect_structure(df_btc)
        return bias
    except Exception:
        return "RANGING"


def macro_score(pair: str, direction: str, btc_bias: str, btcd_trend: str) -> tuple:
    if pair in ALTCOIN_EXEMPTIONS:
        return 0, "BTC/ETH exempt"
    if btc_bias == "RANGING" or btcd_trend == "FLAT":
        return 0, "Macro data insufficient"

    if direction == "BULLISH":
        if btc_bias == "BULLISH" and btcd_trend == "FALLING":
            return SCORE_MACRO_ALIGNED, "BTC Bull + BTC.D↓ → Alt season ✅"
        if btc_bias == "BEARISH" and btcd_trend == "RISING":
            return SCORE_MACRO_CONFLICT, "BTC Bear + BTC.D↑ → Alts bleeding ⚠️"
        if btc_bias == "BULLISH" and btcd_trend == "RISING":
            return SCORE_MACRO_CONFLICT // 2, "BTC Bull + BTC.D↑ → BTC season"
        return 0, "Macro neutral"

    if direction == "BEARISH":
        if btc_bias == "BEARISH" and btcd_trend == "RISING":
            return SCORE_MACRO_ALIGNED, "BTC Bear + BTC.D↑ → Alt bleed ✅"
        if btc_bias == "BULLISH" and btcd_trend == "FALLING":
            return SCORE_MACRO_CONFLICT, "BTC Bull + BTC.D↓ → Alt season, avoid SHORT ⚠️"
        if btc_bias == "BEARISH" and btcd_trend == "FALLING":
            return SCORE_MACRO_CONFLICT // 2, "BTC Bear + BTC.D↓ → Alts may bounce"
        return 0, "Macro neutral"

    return 0, "No match"


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 11b — BTC/BTCD LOW TIMEFRAME CORRELATION FILTER
# ═══════════════════════════════════════════════════════════════════════════

# Cache BTC LTF data agar tidak re-fetch setiap pair
_btc_ltf_cache: dict = {"data": None, "ts": 0.0, "tf": None}

# Timeframe yang digunakan untuk analisis LTF BTC/BTCD
BTC_LTF_TF = "15m"          # timeframe LTF untuk BTC price
BTCD_LTF_CANDLES = 20       # jumlah periode untuk deteksi trend BTCD LTF
BTC_LTF_CANDLES  = 20       # jumlah periode untuk deteksi trend BTC LTF
BTC_LTF_CACHE_TTL = 60      # cache TTL detik (1 menit — sinkron dengan scan)

# Skor filter sinyal dari korelasi BTC/BTCD LTF
# Positif = korelasi mendukung sinyal → bonus score
# Negatif = korelasi berlawanan → penalty atau blok
BTCD_CORR_BONUS   = 12    # kondisi ideal → bonus skor
BTCD_CORR_NEUTRAL = 0     # kondisi netral → tidak ada efek
BTCD_CORR_PENALTY = -15   # kondisi berlawanan → penalty skor
BTCD_CORR_BLOCK   = -999  # kondisi sangat berlawanan → blok sinyal


def fetch_btc_ltf_data(tf: str = BTC_LTF_TF, limit: int = 60) -> Optional[pd.DataFrame]:
    """
    Ambil OHLCV BTC/USDT untuk low timeframe dengan cache 60 detik.
    Digunakan untuk analisis BTC trend jangka pendek.
    """
    global _btc_ltf_cache
    now = time.time()
    if (
        _btc_ltf_cache["data"] is not None
        and _btc_ltf_cache["tf"] == tf
        and (now - _btc_ltf_cache["ts"]) < BTC_LTF_CACHE_TTL
    ):
        return _btc_ltf_cache["data"]
    try:
        df = fetch_ohlcv("BTC/USDT", tf, limit=limit)
        _btc_ltf_cache = {"data": df, "ts": now, "tf": tf}
        return df
    except Exception as e:
        print(f"  ⚠️  BTC LTF fetch error ({tf}): {e}")
        return None


def get_btc_ltf_direction(df_btc_ltf: Optional[pd.DataFrame], period: int = BTC_LTF_CANDLES) -> str:
    """
    Deteksi arah BTC LTF berdasarkan:
    - EMA slope (EMA cepat vs EMA lambat)
    - Candle momentum (N candle terakhir)
    Return: 'BULLISH', 'BEARISH', atau 'FLAT'
    """
    if df_btc_ltf is None or len(df_btc_ltf) < period + 5:
        return "FLAT"

    closes = df_btc_ltf["close"].values
    # EMA cepat (7) vs EMA lambat (20)
    def ema_calc(data, n):
        k = 2 / (n + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e

    recent = closes[-(period + 5):]
    ema_fast = ema_calc(recent, 7)
    ema_slow = ema_calc(recent, 20)

    # Momentum: berapa candle dari 5 terakhir yang searah
    last5 = df_btc_ltf.iloc[-5:]
    bull_count = sum(1 for _, r in last5.iterrows() if float(r["close"]) > float(r["open"]))
    bear_count = 5 - bull_count

    # Kombinasi EMA slope + momentum
    if ema_fast > ema_slow * 1.0005 and bull_count >= 3:
        return "BULLISH"
    if ema_fast < ema_slow * 0.9995 and bear_count >= 3:
        return "BEARISH"
    return "FLAT"


def get_btcd_ltf_direction(df_btcd: Optional[pd.DataFrame], period: int = BTCD_LTF_CANDLES) -> str:
    """
    Deteksi arah BTCD LTF dari DataFrame OHLCV BTCDOMUSDT (Binance 15m).
    Sama persis dengan get_btc_ltf_direction — pakai EMA slope + momentum candle.
    Return: 'RISING', 'FALLING', atau 'FLAT'
    """
    if df_btcd is None or len(df_btcd) < period + 5:
        return "FLAT"

    closes = df_btcd["close"].values

    def ema_calc(data, n):
        k = 2 / (n + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e

    recent   = closes[-(period + 5):]
    ema_fast = ema_calc(recent, 7)
    ema_slow = ema_calc(recent, 20)

    last5      = df_btcd.iloc[-5:]
    bull_count = sum(1 for _, r in last5.iterrows() if float(r["close"]) > float(r["open"]))
    bear_count = 5 - bull_count

    if ema_fast > ema_slow * 1.0005 and bull_count >= 3:
        return "RISING"
    if ema_fast < ema_slow * 0.9995 and bear_count >= 3:
        return "FALLING"
    return "FLAT"



def analyze_btc_situation(
    df_btc_ltf: Optional[pd.DataFrame],
    df_btc_htf: Optional[pd.DataFrame],
    btc_bias_htf: str,
    btcd_ltf_dir: str,
    btcd_trend_htf: str,
) -> dict:
    """
    ═══════════════════════════════════════════════════════════════════════
    BTC SITUATIONAL AWARENESS — Smart Macro Context untuk Alt Signals
    ═══════════════════════════════════════════════════════════════════════

    Tujuan: Deteksi kondisi BTC yang "menjebak" sinyal alt — terutama:

    KASUS UTAMA yang ditangani:
    1. BTC NEAR DEMAND + BEARISH LTF → BTC mau bounce → SHORT alt berbahaya
       (alt SHORT bisa ketarik naik karena BTC rebound dari demand)

    2. BTC NEAR SUPPLY + BULLISH LTF → BTC mau reject → LONG alt berbahaya
       (alt LONG bisa tertekan karena BTC gagal di supply)

    3. BTC EXTENDED (jauh dari structure) → momentum lemah → semua sinyal
       harus lebih selektif (bonus score lebih ketat)

    4. BTC STRONG TRENDING → kondisi ideal → bonus score untuk arah searah

    Output dict:
    {
        "situation": str,         # label kondisi: "BTC_NEAR_DEMAND", "BTC_NEAR_SUPPLY",
                                  #   "BTC_TRENDING_BULLISH", "BTC_TRENDING_BEARISH",
                                  #   "BTC_EXTENDED", "BTC_NEUTRAL"
        "score_adj": int,         # bonus/penalty untuk scoring sinyal alt
        "block_short": bool,      # True = blok SHORT alt (BTC mau bounce)
        "block_long":  bool,      # True = blok LONG alt (BTC mau reject)
        "reason": str,            # penjelasan ringkas untuk log
        "warn_short": bool,       # True = SHORT alt berisiko (peringatan, tidak blok)
        "warn_long":  bool,       # True = LONG alt berisiko (peringatan, tidak blok)
    }
    ═══════════════════════════════════════════════════════════════════════
    """
    result = {
        "situation": "BTC_NEUTRAL",
        "score_adj": 0,
        "block_short": False,
        "block_long":  False,
        "warn_short":  False,
        "warn_long":   False,
        "reason": "BTC situasi netral",
    }

    if df_btc_ltf is None or len(df_btc_ltf) < 30:
        result["reason"] = "BTC LTF data tidak cukup — skip situational check"
        return result

    # ── Ambil data harga BTC ───────────────────────────────────────────────────
    current_price = float(df_btc_ltf["close"].iloc[-1])

    # ── Deteksi swing high/low BTC HTF (demand/supply zone proxy) ─────────────
    df_for_swing = df_btc_htf if (df_btc_htf is not None and len(df_btc_htf) >= 30) else df_btc_ltf
    htf_highs, htf_lows = find_swings(df_for_swing, window=5)

    # Nearest swing low (demand proxy) dan swing high (supply proxy)
    recent_lows  = sorted([p for _, p in htf_lows],  reverse=False)  # ascending
    recent_highs = sorted([p for _, p in htf_highs], reverse=True)   # descending

    nearest_demand = recent_lows[-1]  if recent_lows  else None  # highest recent low = demand
    nearest_supply = recent_highs[-1] if recent_highs else None  # lowest recent high = supply

    # ── ATR BTC untuk ukur "dekat" ────────────────────────────────────────────
    btc_atr = calculate_atr(df_btc_ltf, period=14)
    proximity_factor = 1.5   # dalam 1.5× ATR = "dekat" zona

    near_demand = (
        nearest_demand is not None
        and current_price <= nearest_demand + btc_atr * proximity_factor
        and current_price >= nearest_demand - btc_atr * 0.5
    )
    near_supply = (
        nearest_supply is not None
        and current_price >= nearest_supply - btc_atr * proximity_factor
        and current_price <= nearest_supply + btc_atr * 0.5
    )

    # ── BTC LTF momentum ──────────────────────────────────────────────────────
    btc_ltf_dir = get_btc_ltf_direction(df_btc_ltf)

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 1: BTC dekat DEMAND + LTF mulai berbalik / masih turun
    # → BTC mau bounce → SHORT alt sangat berbahaya
    # ─────────────────────────────────────────────────────────────────────────
    if near_demand and btc_bias_htf == "BEARISH":
        if btc_ltf_dir in ("BULLISH", "FLAT"):
            # LTF sudah mulai balik → bounce hampir terjadi → BLOK SHORT
            result.update({
                "situation": "BTC_NEAR_DEMAND_BOUNCE",
                "score_adj": -20,
                "block_short": True,
                "warn_long":   False,
                "reason": (
                    f"⚠️  BTC HTF BEARISH tapi sudah di demand zone "
                    f"(~{nearest_demand:.0f}) + LTF {btc_ltf_dir} → bounce imminent "
                    f"→ SHORT alt DIBLOK (ketarik naik)"
                ),
            })
            return result
        else:
            # LTF masih turun tapi harga sudah di demand → waspada SHORT
            result.update({
                "situation": "BTC_NEAR_DEMAND_FALLING",
                "score_adj": -10,
                "warn_short": True,
                "reason": (
                    f"⚠️  BTC di demand zone (~{nearest_demand:.0f}) + LTF masih turun "
                    f"→ SHORT alt berisiko, bisa snap back kapan saja"
                ),
            })
            return result

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 2: BTC dekat SUPPLY + LTF mulai melemah / masih naik
    # → BTC mau reject → LONG alt berbahaya
    # ─────────────────────────────────────────────────────────────────────────
    if near_supply and btc_bias_htf == "BULLISH":
        if btc_ltf_dir in ("BEARISH", "FLAT"):
            # LTF sudah mulai balik → rejection hampir terjadi → BLOK LONG
            result.update({
                "situation": "BTC_NEAR_SUPPLY_REJECT",
                "score_adj": -20,
                "block_long": True,
                "warn_short": False,
                "reason": (
                    f"⚠️  BTC HTF BULLISH tapi sudah di supply zone "
                    f"(~{nearest_supply:.0f}) + LTF {btc_ltf_dir} → rejection imminent "
                    f"→ LONG alt DIBLOK (kena tekanan BTC turun)"
                ),
            })
            return result
        else:
            # LTF masih naik tapi di supply → waspada LONG
            result.update({
                "situation": "BTC_NEAR_SUPPLY_RISING",
                "score_adj": -8,
                "warn_long": True,
                "reason": (
                    f"⚠️  BTC di supply zone (~{nearest_supply:.0f}) + LTF masih naik "
                    f"→ LONG alt berisiko, BTC bisa reject kapan saja"
                ),
            })
            return result

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 3: BTC STRONG TREND — kondisi ideal (mid-trend, bukan di ekstrem)
    # ─────────────────────────────────────────────────────────────────────────
    if btc_bias_htf == "BULLISH" and btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
        result.update({
            "situation": "BTC_TRENDING_BULLISH",
            "score_adj": +8,
            "reason": "✅ BTC HTF BULLISH + LTF naik + BTCD turun → Alt season momentum → bonus LONG",
        })
        return result

    if btc_bias_htf == "BEARISH" and btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
        result.update({
            "situation": "BTC_TRENDING_BEARISH",
            "score_adj": +8,
            "reason": "✅ BTC HTF BEARISH + LTF turun + BTCD naik → Alt bleed momentum → bonus SHORT",
        })
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # KASUS 4: BTC EXTENDED — harga jauh dari struktur, reversion risk tinggi
    # Heuristik: jika tidak near demand/supply tapi juga tidak trending jelas
    # ─────────────────────────────────────────────────────────────────────────
    if btc_bias_htf == "RANGING" and btc_ltf_dir == "FLAT":
        result.update({
            "situation": "BTC_EXTENDED",
            "score_adj": -5,
            "reason": "⚡ BTC ranging + LTF flat → pasar konsolidasi, sinyal alt lebih berisiko",
        })
        return result

    # Default: netral
    result["reason"] = f"BTC HTF={btc_bias_htf} LTF={btc_ltf_dir} BTCD={btcd_ltf_dir} → situasi netral"
    return result

def analyze_btcd_correlation(
    direction: str,
    btc_ltf_dir: str,
    btcd_ltf_dir: str,
    btc_bias_htf: str,
    btcd_trend_htf: str,
    pair: str,
) -> tuple:
    """
    Analisis korelasi BTC + BTC Dominance untuk menentukan apakah sinyal
    layak dikirim berdasarkan kondisi makro LTF.

    LOGIKA UTAMA:
    ─────────────────────────────────────────────────────────────────
    LONG (alt coin naik):
      ✅ IDEAL      : BTC LTF naik  + BTCD LTF turun  → Alt season  → +bonus
      ✅ OK         : BTC LTF naik  + BTCD LTF flat   → BTC rally, neutral alt
      ⚠️ WASPADA   : BTC LTF flat  + BTCD LTF turun  → Mungkin alt season
      ❌ KONFLIK    : BTC LTF turun + BTCD LTF naik   → Alts bleeding → penalty
      🚫 BLOK      : BTC LTF turun + BTCD LTF naik (keduanya kuat) → blok

    SHORT (alt coin turun):
      ✅ IDEAL      : BTC LTF turun + BTCD LTF naik   → Alt bleed   → +bonus
      ✅ OK         : BTC LTF turun + BTCD LTF flat   → BTC dump, alts ikut
      ⚠️ WASPADA   : BTC LTF flat  + BTCD LTF naik   → Alt bleed mungkin
      ❌ KONFLIK    : BTC LTF naik  + BTCD LTF turun  → Alt season  → penalty
      🚫 BLOK      : BTC LTF naik  + BTCD LTF turun (keduanya kuat) → blok

    BTC/ETH: hanya cek BTC LTF (tidak terpengaruh BTCD)
    ─────────────────────────────────────────────────────────────────
    Return: (score_adj: int, reason: str, signal_blocked: bool)
    """
    # BTC/ETH: korelasi BTCD tidak relevan, hanya cek momentum LTF
    if pair in ALTCOIN_EXEMPTIONS:
        if direction == "BULLISH" and btc_ltf_dir == "BULLISH":
            return BTCD_CORR_BONUS, "BTC LTF↑ — momentum konfirmasi LONG ✅", False
        if direction == "BEARISH" and btc_ltf_dir == "BEARISH":
            return BTCD_CORR_BONUS, "BTC LTF↓ — momentum konfirmasi SHORT ✅", False
        if btc_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC LTF flat — netral", False
        # Berlawanan
        dir_str = "LONG" if direction == "BULLISH" else "SHORT"
        return BTCD_CORR_PENALTY, f"BTC LTF berlawanan arah {dir_str} ⚠️", False

    # Altcoin: cek kombinasi BTC LTF + BTCD LTF
    if direction == "BULLISH":
        # ── KONDISI IDEAL untuk LONG alt ──────────────────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_BONUS, "BTC LTF↑ + BTCD LTF↓ → Alt season LTF ✅ IDEAL untuk LONG", False

        # ── Kondisi OK ────────────────────────────────────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC LTF↑ + BTCD flat → BTC rally, alt netral ✅", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD LTF↓ → Mungkin alt season ⚡", False

        # ── Kondisi netral / ambigu ───────────────────────────────────────────
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD flat → Pasar netral", False
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_NEUTRAL // 2 if BTCD_CORR_NEUTRAL != 0 else 0, \
                   "BTC LTF↑ + BTCD LTF↑ → BTC season, alt tertinggal ⚠️", False

        # ── Kondisi berbahaya untuk LONG ──────────────────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_PENALTY // 2, "BTC LTF↓ + BTCD flat → Risiko LONG meningkat ⚠️", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_PENALTY // 2, "BTC flat + BTCD LTF↑ → Alts mulai bleeding ⚠️", False

        # ── BLOK: kondisi paling berbahaya untuk LONG ─────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_BLOCK, "🚫 BTC LTF↓ + BTCD LTF↑ → Alts BLEEDING — LONG DIBLOK", True

        return BTCD_CORR_NEUTRAL, "Korelasi BTC/BTCD netral", False

    if direction == "BEARISH":
        # ── KONDISI IDEAL untuk SHORT alt ─────────────────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_BONUS, "BTC LTF↓ + BTCD LTF↑ → Alt bleed LTF ✅ IDEAL untuk SHORT", False

        # ── Kondisi OK ────────────────────────────────────────────────────────
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC LTF↓ + BTCD flat → BTC dump, alt ikut ✅", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "RISING":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD LTF↑ → Alt bleed mungkin ⚡", False

        # ── Kondisi netral / ambigu ───────────────────────────────────────────
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_NEUTRAL, "BTC flat + BTCD flat → Pasar netral", False
        if btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_NEUTRAL, "BTC LTF↓ + BTCD LTF↓ → Alts mungkin bounce ⚠️", False

        # ── Kondisi berbahaya untuk SHORT ─────────────────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FLAT":
            return BTCD_CORR_PENALTY // 2, "BTC LTF↑ + BTCD flat → Risiko SHORT meningkat ⚠️", False
        if btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_PENALTY // 2, "BTC flat + BTCD LTF↓ → Alts mulai recover ⚠️", False

        # ── BLOK: kondisi paling berbahaya untuk SHORT ────────────────────────
        if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
            return BTCD_CORR_BLOCK, "🚫 BTC LTF↑ + BTCD LTF↓ → Alt SEASON — SHORT DIBLOK", True

        return BTCD_CORR_NEUTRAL, "Korelasi BTC/BTCD netral", False

    return BTCD_CORR_NEUTRAL, "Direction tidak dikenal", False


def get_btcd_correlation_summary(
    btc_ltf_dir: str,
    btcd_ltf_dir: str,
    btc_bias_htf: str,
    btcd_trend_htf: str,
) -> str:
    """
    Buat ringkasan teks kondisi BTC/BTCD untuk laporan /btcdcorrelation.
    """
    lines = []

    # HTF context
    btc_em  = "📈" if btc_bias_htf == "BULLISH" else ("📉" if btc_bias_htf == "BEARISH" else "➡️")
    btcd_em = "📈" if btcd_trend_htf == "RISING" else ("📉" if btcd_trend_htf == "FALLING" else "➡️")
    lines.append(f"{btc_em} BTC HTF  : <b>{btc_bias_htf}</b>")
    lines.append(f"{btcd_em} BTCD HTF : <b>{btcd_trend_htf}</b>")
    lines.append("─" * 34)

    # LTF context
    btc_ltf_em  = "📈" if btc_ltf_dir == "BULLISH" else ("📉" if btc_ltf_dir == "BEARISH" else "➡️")
    btcd_ltf_em = "📈" if btcd_ltf_dir == "RISING" else ("📉" if btcd_ltf_dir == "FALLING" else "➡️")
    lines.append(f"{btc_ltf_em} BTC LTF  ({BTC_LTF_TF}): <b>{btc_ltf_dir}</b>")
    lines.append(f"{btcd_ltf_em} BTCD LTF ({BTC_LTF_TF}): <b>{btcd_ltf_dir}</b>")
    lines.append("─" * 34)

    # Interpretasi
    if btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING":
        lines.append("🟢 <b>ALT SEASON MODE</b>")
        lines.append("   BTC naik, dominance turun → Altcoin pump")
        lines.append("   ✅ LONG alt sangat favorable")
        lines.append("   🚫 SHORT alt diblok")
    elif btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING":
        lines.append("🔴 <b>ALT BLEEDING MODE</b>")
        lines.append("   BTC turun, dominance naik → Altcoin dump")
        lines.append("   ✅ SHORT alt sangat favorable")
        lines.append("   🚫 LONG alt diblok")
    elif btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "RISING":
        lines.append("🟡 <b>BTC SEASON MODE</b>")
        lines.append("   BTC naik, dominance naik → Modal masuk BTC")
        lines.append("   ⚠️ Alt tertinggal — selektif LONG")
        lines.append("   ✅ SHORT alt boleh (relatif underperform)")
    elif btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "FALLING":
        lines.append("🟡 <b>MARKET DUMP MODE</b>")
        lines.append("   BTC turun, dominance turun → Semua turun")
        lines.append("   ⚠️ Alt mungkin bounce relatif terhadap BTC")
        lines.append("   ⚠️ SHORT alt dengan hati-hati")
    elif btc_ltf_dir == "FLAT" and btcd_ltf_dir == "FLAT":
        lines.append("⚪ <b>KONSOLIDASI / NETRAL</b>")
        lines.append("   Tidak ada sinyal kuat dari BTC/BTCD")
        lines.append("   ⚡ Sinyal berdasarkan SMC murni")
    elif btc_ltf_dir == "BULLISH":
        lines.append("🟢 <b>BTC MOMENTUM NAIK</b>")
        lines.append(f"   BTCD {btcd_ltf_dir} — kondisi campur")
        lines.append("   ✅ LONG tetap dipertimbangkan")
    elif btc_ltf_dir == "BEARISH":
        lines.append("🔴 <b>BTC MOMENTUM TURUN</b>")
        lines.append(f"   BTCD {btcd_ltf_dir} — kondisi campur")
        lines.append("   ⚠️ Waspadai posisi LONG alt")
    else:
        lines.append("⚪ <b>KONDISI CAMPURAN</b>")
        lines.append("   Korelasi BTC/BTCD tidak konklusif")

    lines.append("─" * 34)

    # Rekomendasi
    long_ok  = not (btc_ltf_dir == "BEARISH" and btcd_ltf_dir == "RISING")
    short_ok = not (btc_ltf_dir == "BULLISH" and btcd_ltf_dir == "FALLING")
    lines.append(f"📋 <b>Rekomendasi Saat Ini:</b>")
    lines.append(f"   LONG  alt : {'✅ OK' if long_ok  else '🚫 DIBLOK'}")
    lines.append(f"   SHORT alt : {'✅ OK' if short_ok else '🚫 DIBLOK'}")

    return "\n".join(lines)


def send_btcd_correlation_report(chat_id: str = None):
    """
    Kirim laporan lengkap kondisi BTC/BTCD LTF + HTF ke Telegram.
    Dipanggil oleh command /btcdcorrelation.
    """
    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")

    # Fetch data
    try:
        df_btc_ltf = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
        btc_ltf_dir = get_btc_ltf_direction(df_btc_ltf)
    except Exception as e:
        btc_ltf_dir = "FLAT"
        print(f"  ⚠️  BTC LTF error: {e}")

    try:
        df_btcd      = fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)
        btcd_ltf_dir = get_btcd_ltf_direction(df_btcd)
        # HTF: fetch dengan timeframe lebih besar untuk trend
        df_btcd_htf  = fetch_btcd_ohlcv(tf=BTCDOM_HTF_TF, limit=60)
        btcd_htf_dir = get_btcd_trend(df_btcd_htf)
    except Exception as e:
        btcd_ltf_dir = "FLAT"
        btcd_htf_dir = "FLAT"
        print(f"  ⚠️  BTCD fetch error: {e}")

    try:
        btc_bias_htf = get_btc_bias()
    except Exception:
        btc_bias_htf = "RANGING"

    summary = get_btcd_correlation_summary(btc_ltf_dir, btcd_ltf_dir, btc_bias_htf, btcd_htf_dir)

    # Contoh dampak per skenario
    _, long_reason, long_blocked  = analyze_btcd_correlation(
        "BULLISH", btc_ltf_dir, btcd_ltf_dir, btc_bias_htf, btcd_htf_dir, "SOL/USDT"
    )
    _, short_reason, short_blocked = analyze_btcd_correlation(
        "BEARISH", btc_ltf_dir, btcd_ltf_dir, btc_bias_htf, btcd_htf_dir, "SOL/USDT"
    )

    msg = (
        f"🔗 <b>BTC/BTCD CORRELATION REPORT</b>\n"
        f"📅 {now_str}\n"
        f"{'═' * 34}\n"
        f"{summary}\n"
        f"{'═' * 34}\n"
        f"📊 <b>Dampak ke Sinyal Alt (contoh SOL):</b>\n"
        f"   LONG  : {long_reason}\n"
        f"   SHORT : {short_reason}\n"
        f"{'─' * 34}\n"
        f"ℹ️ Filter ini aktif otomatis di setiap sinyal.\n"
        f"   BTC LTF TF: {BTC_LTF_TF} | BTCD: Binance {BTCDOM_TICKER} {BTCDOM_LTF_TF}"
    )
    _tg_send(msg, chat_id)
    print(f"  🔗 BTC/BTCD correlation report dikirim → {chat_id or TELEGRAM_CHAT_ID}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 12 — SESSION
# ═══════════════════════════════════════════════════════════════════════════

def get_session() -> str:
    h = datetime.now(timezone.utc).hour
    if 7  <= h < 13: return "London"
    if 12 <= h < 21: return "New York"
    if 0  <= h < 8:  return "Asia"
    return "Off-Hours"


def session_score(session: str) -> int:
    """
    ALIGNED WITH BASE: London/NY +5, sesi lain (termasuk Asia) 0.
    Penalti khusus Asia (-3) dihapus — tidak ada di BASE.
    """
    return SCORE_SESSION if session in ("London", "New York") else 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 13 — ATR & RISK-REWARD
# ═══════════════════════════════════════════════════════════════════════════

def calculate_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> float:
    trs = []
    for i in range(1, len(df)):
        h  = float(df.iloc[i]["high"])
        l  = float(df.iloc[i]["low"])
        pc = float(df.iloc[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if not trs:
        return float(df["high"].iloc[-1] - df["low"].iloc[-1])
    return sum(trs[-period:]) / min(period, len(trs))


def calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    ADX Wilder (0-100). FIX 2026-11-10: versi lama memakai running-sum untuk
    smoothing akhir sehingga nilainya ~14x terlalu besar (mis. 842) → label
    "RANGING" tidak pernah muncul. Sekarang delegasi ke calculate_adx_di().
    """
    return calculate_adx_di(df, period)[0]


def calculate_adx_di(df: pd.DataFrame, period: int = 14) -> tuple:
    """
    Wilder ADX + DI. Return (adx, +DI, -DI) nilai terakhir.
    Data kurang → (25.0, 0.0, 0.0) supaya gate TIDAK memblok karena data pendek.
    """
    if len(df) < period * 2 + 1:
        return 25.0, 0.0, 0.0
    highs  = df["high"].astype(float).values
    lows   = df["low"].astype(float).values
    closes = df["close"].astype(float).values
    tr_l, pdm_l, ndm_l = [], [], []
    for i in range(1, len(df)):
        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        tr_l.append(max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])))
        pdm_l.append(up   if (up > down and up > 0)   else 0.0)
        ndm_l.append(down if (down > up and down > 0) else 0.0)

    def _ws(data, n):
        out = [sum(data[:n])]
        for v in data[n:]:
            out.append(out[-1] - out[-1] / n + v)
        return out

    atr_s, pdm_s, ndm_s = _ws(tr_l, period), _ws(pdm_l, period), _ws(ndm_l, period)
    dx, pdi, ndi = [], 0.0, 0.0
    for i in range(len(atr_s)):
        pdi = 100 * pdm_s[i] / atr_s[i] if atr_s[i] > 0 else 0.0
        ndi = 100 * ndm_s[i] / atr_s[i] if atr_s[i] > 0 else 0.0
        den = pdi + ndi
        dx.append(100 * abs(pdi - ndi) / den if den > 0 else 0.0)
    # ADX = Wilder AVERAGE dari DX (bukan running-sum — running-sum membuat ADX
    # membengkak ~14x, mis. 842, sehingga market choppy tidak pernah terdeteksi).
    if len(dx) < period:
        return 25.0, pdi, ndi
    adx = sum(dx[:period]) / period
    for v in dx[period:]:
        adx = (adx * (period - 1) + v) / period
    return adx, pdi, ndi


# ── ADX Regime Detection (2026-09-15) ───────────────────────────────────────
# Diaktifkan kembali sebagai SOFT-SCORE, bukan hard gate — permintaan user
# eksplisit: market lagi ranging (koin gak naik-gak turun), tapi tidak mau
# bot jadi terlalu ketat / sinyal makin jarang keluar. calculate_adx() sendiri
# sudah ada sejak lama tapi dead code (LEGACY, tidak pernah dipanggil).
#
# Desain: ADX cuma menambah SATU komponen baru bd["regime"] ke compute_score()
# — bukan gate/return terpisah. Setup yang tetap kuat di komponen lain masih
# bisa lolos threshold walau regime kurang ideal untuk gayanya.
#   • ADX rendah (< ADX_RANGING_THRESHOLD)  → market ranging/choppy:
#       - RSI extreme (>75 / <25, sama definisi rsi_score()) → bonus kecil,
#         karena mean-reversion di area jenuh lebih reliable saat sideways.
#       - momentum_aligned (chasing breakout searah trend) → penalty kecil,
#         karena breakout di ranging market lebih sering fakeout/whipsaw.
#   • ADX tinggi (>= ADX_TRENDING_MIN) → market trending:
#       - momentum_aligned → bonus kecil, breakout/continuation lebih reliable.
# Kombinasi lain (mis. ADX tinggi tanpa momentum_aligned) → 0, netral.
ADX_TRENDING_MIN      = 20
ADX_TRENDING_FULL      = 25

# ── TREND GATE (2026-11-10) ─────────────────────────────────────────────────
# Tujuan: bot hanya entry saat market BENAR-BENAR trending DAN searah dengan
# arah trade. Latar belakang: win datang saat market trending terus (naik/turun),
# loss menumpuk saat market di tengah/choppy; struktur HTF (detect_structure)
# lagging, jadi LONG sering keluar saat harga sebenarnya sudah turun (LONG saat
# HTF BULLISH hanya WR 12%, n=56). Dihitung di df_htf (1h untuk LOW_TF).
#   • TREND_GATE_ADX_MIN  : skip kalau ADX(HTF) < nilai ini (0 = nonaktif)
#   • TREND_GATE_DI_ALIGN : skip kalau +DI/-DI HTF tidak searah arah trade
# Simetris untuk LONG & SHORT (tidak ada bias arah). Matikan lewat BOT_OVERRIDES
# di backtest.py untuk A/B test.
TREND_GATE_ON         = True
# Blok SEMUA entry saat HTF RANGING (sideways) — tempat fakeout paling sering.
# True  = langsung skip (hemat juga 1 panggilan API). False = perilaku lama (adaptive).
# Dasar: backtest 13 Sep–8 Okt 2026, 3 trade RANGING → 1 menang, total -20 USDT (sampel kecil).
BLOCK_HTF_RANGING     = True
TREND_GATE_ADX_MIN    = 20.0
TREND_GATE_DI_ALIGN   = True

# ── EDGE FILTER (2026-11-11, v70) ───────────────────────────────────────────
# Dasar: backtest 30 hari (127 trade, PF 0.99). Dua fitur memisahkan loser dari
# winner secara konsisten di KEDUA paruh periode (bukan cuma total):
#   1) ADX(HTF) > 32 → tren sudah extended/telat. n=63: -0.22R/trade (ADX<=30:
#      +0.21R). Pola sama di paruh 1 dan 2. Plateau di cap 30-35, bukan knife-edge.
#   2) Entry di zona FVG-saja (tanpa OB) → n=51: -0.26R/trade; entry OB atau
#      market-pullback (tanpa zona) jauh lebih baik. Stabil di kedua paruh.
# Hasil backtest ulang (periode SAMA 12 Okt-11 Nov, 15 pair): 88 trade, WR 67%,
# PF 1.22, E +0.08R, PnL +66.6 USDT, maxDD 12.5% (sebelumnya PF 0.99, -7.2 USDT).
# Trade yang lolos filter sejak awal: +0.32R; trade BARU yang mengisi slot kosong:
# -0.21R → edge belum terbukti. STATUS: BELUM TERVALIDASI di periode lain.
# Wajib backtest --end periode lain + forward-test demo >= 100 trade sebelum
# live. Matikan lewat BOT_OVERRIDES di backtest.py untuk A/B test:
#   TREND_GATE_ADX_MAX = 0            → cap ADX nonaktif
#   ENTRY_BLOCK_FVG_ONLY = False      → entry FVG-saja boleh lagi
TREND_GATE_ADX_MAX    = 32.0   # skip kalau ADX(HTF) > nilai ini (0 = nonaktif)
ENTRY_BLOCK_FVG_ONLY  = True   # skip kalau harga di FVG tapi TIDAK di OB

# ── LOSS-DRIVER FILTERS (2026-11-17) — dari analisa 2 backtest log (n=17, SHORT semua) ──
# Temuan: DI gap HTF <= 12 → 1 dari 9 win; > 12 → 5 dari 8. RSI entry >= 50 pada SHORT → 0 dari 4 win.
# SAMPEL KECIL & belum tervalidasi — A/B dulu lewat BOT_OVERRIDES di backtest.py (100+ trade)
# sebelum live. Matikan: TREND_GATE_DI_MIN = 0 / SHORT_RSI_MAX_ENTRY = 0.
TREND_GATE_DI_MIN     = 0.0    # skip kalau kekuatan DI gap HTF (searah trade) < nilai ini (0 = nonaktif)
# ── MODE RISET (2026-11-17) — HANYA UNTUK BACKTEST, JANGAN DIPAKAI LIVE ──────────────────────
# True = gate pemfilter lama dimatikan: RANGING block + LTF-score gate, trend gate ADX/DI, slow trend,
# HTF structure, edge filter (arah/ATR/pair), FVG-only, entry-zone, dan tolak LONG di sesi Asia.
# TETAP aktif: skor minimum, BOS NONE, volatilitas minimum, cooldown sinyal per pair, posisi aktif per pair,
# serta STRUCT_FILTER_LTF (kalau dinyalakan lewat token `sf1`) dan limit harian/mingguan/bulanan (kalau diberi token).
# Nilai input tiap gate dicatat ke CSV (adx_htf, di_gap_htf, ema_gap_htf, atr_pct, htf_bias,
# htf_struct_ok, htf_er, sl_source=OB/FVG/SWING) untuk dinilai belakangan.
RESEARCH_MODE         = False   # v-retest: default MATI. Di backtest nyalakan lewat token `rs1` (jangan True di bot live)

# ── ZONA RETEST HTF (v-retest 2026-11-17) ────────────────────────────────────────────────────
# Entry di support yang jebol jadi resistance (SHORT) / resistance yang jebol jadi support (LONG) dari swing 1h:
#   zona = level swing 1h (pivot 3/3) yang sudah ditembus close, terdekat dari harga di sisi retest,
#   dilebarkan RZ_UP_ATR (sisi retest) dan RZ_BACK_ATR (sisi dalam) x ATR 1h. SL tepat di luar zona
#   (+ RZ_SL_BUF_ATR x ATR entry TF), TP = SINGLE_TP_R_MULT. BOS 30m tidak wajib saat retest HTF.
# 0 = mati (perilaku lama) | 1 = HANYA zona retest HTF | 2 = OB/FVG 30m ATAU zona retest HTF
# Di backtest: token `rz1` / `rz2` (tanpa ganti file).
HTF_RETEST_ZONE_MODE  = 0
RZ_UP_ATR             = 0.5
RZ_BACK_ATR           = 0.25
RZ_SL_BUF_ATR         = 0.3
# Sumber zona: 0 = band di sekitar level swing 1h yang jebol (v1) | 1 = OB gaya LuxAlgo di 1h (HTF) |
#              2 = OB gaya LuxAlgo di entry TF (30m). Di backtest: token `zs0` / `zs1` / `zs2`.
HTF_RETEST_ZONE_SRC   = 1

# ── REGIME WAJIB SEARAH (v-regime 2026-11-17) ────────────────────────────────────────────────
# Dari analisa 1309 trade (6 window): trade searah regime pasar (LONG di BULL_REGIME, SHORT di BEAR_REGIME) R +0.27,
# MIXED +0.02. True = hanya entry kalau regime searah (BTC/ETH dikecualikan). Beda dengan EDGE_REGIME_GATE_ON
# yang cuma memblok yang MELAWAN regime (hampir tak pernah terjadi). Di backtest: token `rg1`. Default MATI.
REGIME_REQUIRE_ALIGNED = False

# ── LONG DI DEMAND TIMEFRAME TINGGI + REAKSI LTF (v-dz 2026-11-17) ───────────────────────────
# Ide: LONG hanya saat harga baru menyentuh zona DEMAND (OB bullish gaya LuxAlgo, belum termitigasi) di 1D / 1W,
# lalu menunggu REAKSI BUYER di entry TF. (2026-11-24: TIDAK lagi butuh BOS / HH+HL 30m. Cukup:
#   (1) harga sempat masuk zona demand, (2) ada reaksi buyer, (3) candle 30m terakhir yang sudah close = bullish.)
# SL di bawah dasar zona / wick terendah sentuhan (+ buffer RZ_SL_BUF_ATR x ATR), TP = SINGLE_TP_R_MULT.
# Jalur ini MENGGANTIKAN gate arah HTF (ranging, ADX/DI, slow trend, struktur HTF, regime) karena ini setup reversal.
# 0 = mati | 1 = demand 1D | 2 = demand 1W. Di backtest: token `dz1` / `dz2`. Default MATI.
HTF_DEMAND_MODE       = 0
DZ_STATS = {"checks": 0, "fetch_fail": 0, "no_bull_ob": 0, "touched": 0, "after_bos": 0, "after_score": 0, "signals": 0}   # diagnosa (dibaca backtest)
DZ_TOUCH_BARS         = 16      # sentuhan zona dihitung kalau terjadi dalam N candle entry TF terakhir (16 x 30m = 8 jam)
# Reaksi buyer di demand (pengganti syarat BOS/HH+HL). Cukup SALAH SATU:
#   - ada candle yang masuk zona dengan ekor bawah panjang (>= DZ_REACTION_WICK_MIN dari panjang candle) = buyer menolak harga turun
#   - harga sudah naik >= DZ_REACTION_BOUNCE_ATR x ATR dari titik terendah sejak menyentuh zona
DZ_REACTION_WICK_MIN   = 0.30
DZ_REACTION_BOUNCE_ATR = 0.30
DZ_REACTION_MIN_RANGE_ATR = 0.50   # candle yang dinilai ekornya harus cukup besar (>= 0.5 x ATR), biar candle mini tidak dihitung
# True = sinyal demand tidak ditahan skor minimum (skor ikut sistem BOS lama, jadi cuma bikin sinyal demand kepotong).
# Gate risiko lain (RR, volatilitas, cooldown, BTC/BTCD) TETAP jalan. False = skor minimum tetap berlaku.
DZ_SKIP_SCORE_GATE     = True   # berlaku juga untuk SHORT di supply (HTF_SUPPLY_MODE)

# ── SHORT DI SUPPLY (OB BEARISH gaya LuxAlgo = "zona merah") + REAKSI SELLER (2026-11-24) ────────────────
# Kembaran mode demand tapi untuk SHORT: harga baru menyentuh zona SUPPLY (Order Block bearish internal LuxAlgo,
# belum termitigasi = harga belum tembus atap zona), lalu ada REAKSI SELLER di entry TF dan candle close terakhir bearish.
# Tanpa BOS / LH+LL. SL di atas atap zona / wick tertinggi sentuhan (+ buffer RZ_SL_BUF_ATR x ATR), TP = SINGLE_TP_R_MULT.
# Jalur ini MENGGANTIKAN gate arah HTF seperti mode demand (setup reversal).
# 0 = mati | 1 = zona dari chart 1H | 2 = zona 4H | 3 = zona 1D | 4 = zona 1W. Di backtest: token `sz1` .. `sz4`.
HTF_SUPPLY_MODE       = 1       # DINYALAKAN 2026-11-24 (SHORT di zona merah 1H, dengan pengaman SZ_REQUIRE_*). Pakai bersama /maxopentrade 3. Ubah ke 0 untuk mematikan.
SZ_TF_MAP             = {1: "1h", 2: "4h", 3: "1d", 4: "1w"}
SZ_STATS = {"checks": 0, "fetch_fail": 0, "no_bear_ob": 0, "touched": 0, "after_bos": 0, "after_score": 0, "signals": 0,
            "block_di": 0, "block_btc": 0}   # diagnosa (dibaca backtest)
# PENGAMAN SHORT di supply (2026-11-24, dari backtest 30 hari 11 Okt-10 Nov 2024: 218 trade supply, WR 37%, -132 USDT).
# Dua penyebab rugi terbesar: (1) SHORT saat BTC 15m sedang naik (58 trade, WR 15%, -195 USDT) dan
# (2) SHORT padahal tren 1h (DI) melawan (89 trade yang sangat melawan, WR 24%, -208 USDT).
# Dengan dua syarat ini di log yang sama: 96 trade, WR 56%, +172 USDT. BELUM diuji di periode lain.
# Backtest: token `sg0` = pengaman mati (hasil seperti sebelumnya) | `sg1` = nyala. Tanpa token = ikut nilai di bawah.
SZ_REQUIRE_BTC_NOT_UP = True    # SHORT di supply ditolak kalau BTC 15m sedang naik (BULLISH)
SZ_REQUIRE_HTF_DI     = True    # SHORT di supply ditolak kalau tren 1h melawan (selisih DI < SZ_MIN_DI_GAP)
SZ_MIN_DI_GAP         = 0.0     # 0 = tren 1h minimal tidak melawan SHORT; naikkan (mis. 5) kalau mau tren 1h turun jelas
RZ_OB_SIZE            = 5       # size struktur internal LuxAlgo
# Pilih OB: 0 = terdekat dari harga | 1 = 'origin' = OB dengan pergerakan lanjutan terbesar sesudahnya (asal impuls). Token `zp0`/`zp1`.
RZ_OB_PICK            = 1
SHORT_RSI_MAX_ENTRY   = 0.0    # skip SHORT kalau RSI entry >= nilai ini (pullback, bukan lanjut turun; 0 = nonaktif)

# ── PULLBACK EMA 20/50 (2026-11-15) — entry hanya saat harga kembali ke area EMA searah tren ──
# "OFF" = tidak ada efek (default, perilaku lama). "GATE" = tolak sinyal bila bukan pullback ke EMA20/50.
# Aturan (tetap, tidak disetel): EMA20 searah EMA50; dalam EMA_PULLBACK_LOOKBACK candle terakhir harga
# menyentuh area EMA20 (± EMA_PULLBACK_TOUCH_ATR × ATR) dan tidak menutup melewati EMA50 (± EMA_PULLBACK_BREAK_ATR × ATR);
# candle tertutup terakhir searah trade dan close kembali di sisi benar EMA20 (konfirmasi momentum);
# harga sekarang tidak lebih dari EMA_PULLBACK_MAX_CHASE_ATR × ATR dari EMA20 (tidak mengejar).
EMA_PULLBACK_MODE          = "OFF"
EMA_PULLBACK_LOOKBACK      = 6
EMA_PULLBACK_TOUCH_ATR     = 0.25
EMA_PULLBACK_BREAK_ATR     = 0.50
EMA_PULLBACK_MAX_CHASE_ATR = 1.5


def check_ema_pullback(df, direction, atr):
    """(ok, alasan). df = candle entry TF (hanya yang SUDAH close). Tanpa lookahead."""
    try:
        if df is None or len(df) < 60 or not atr or atr <= 0:
            return False, "data/ATR kurang"
        c = df["close"].astype(float)
        e20 = c.ewm(span=20, adjust=False).mean()
        e50 = c.ewm(span=50, adjust=False).mean()
        n = int(EMA_PULLBACK_LOOKBACK)
        hi, lo = df["high"].astype(float), df["low"].astype(float)
        o = float(df["open"].iloc[-1])
        px = float(c.iloc[-1])
        t_ = EMA_PULLBACK_TOUCH_ATR * atr
        b_ = EMA_PULLBACK_BREAK_ATR * atr
        if direction == "BULLISH":
            if not float(e20.iloc[-1]) > float(e50.iloc[-1]):
                return False, "EMA20 tidak di atas EMA50"
            touched = bool((lo.iloc[-n:].to_numpy() <= e20.iloc[-n:].to_numpy() + t_).any())
            broke = bool((c.iloc[-n:].to_numpy() < e50.iloc[-n:].to_numpy() - b_).any())
            mom = px > o and px > float(e20.iloc[-1])
            chase = px - float(e20.iloc[-1]) > EMA_PULLBACK_MAX_CHASE_ATR * atr
        else:
            if not float(e20.iloc[-1]) < float(e50.iloc[-1]):
                return False, "EMA20 tidak di bawah EMA50"
            touched = bool((hi.iloc[-n:].to_numpy() >= e20.iloc[-n:].to_numpy() - t_).any())
            broke = bool((c.iloc[-n:].to_numpy() > e50.iloc[-n:].to_numpy() + b_).any())
            mom = px < o and px < float(e20.iloc[-1])
            chase = float(e20.iloc[-1]) - px > EMA_PULLBACK_MAX_CHASE_ATR * atr
        if not touched:
            return False, "belum pullback ke area EMA20"
        if broke:
            return False, "pullback menembus EMA50"
        if not mom:
            return False, "belum ada candle konfirmasi searah"
        if chase:
            return False, "harga sudah terlalu jauh dari EMA20 (mengejar)"
        return True, "pullback EMA20/50 + konfirmasi"
    except Exception as e:  # noqa: BLE001
        return False, f"error {e}"

# SLOW TREND GATE (2026-11-10): arah trade harus searah rezim LAMBAT pair itu sendiri:
# EMA50 vs EMA200 di HTF (1h → ~2 hari vs ~8 hari). Struktur HTF & DI 14 candle terlalu
# cepat/lagging: LONG saat "HTF+BTC BULLISH + ADX/DI naik" tetap 0 menang dari 15
# trade, karena rezim lambat sedang turun (rebound pendek dibeli, lalu dijual lagi).
# Simetris LONG/SHORT; hanya blok kalau datanya cukup (>=210 candle).
SLOW_TREND_GATE_ON    = True

# ── HTF STRUCTURE GATE (v76) — alt harus punya struktur jelas di HTF ─────────
# Masalah: alt sering SIDEWAYS walau BTC naik/turun; entry di tengah range = fakeout.
# Syarat entry (dihitung di df_htf = 1h untuk LOW_TF, hanya candle yang sudah close):
#   BULLISH lolos jika  (a) BREAKOUT: dalam HTF_STRUCT_RECENT candle terakhir ada close di atas
#                           high range dasar (HTF_STRUCT_LOOKBACK candle sebelumnya) DAN harga
#                           sekarang masih bertahan di atas level itu (toleransi 0.5 ATR), ATAU
#                       (b) STRUKTUR TREN: swing high & swing low terakhir sama-sama naik (HH+HL).
#   BEARISH mirror: BREAKDOWN di bawah low range dasar, atau LH+LL.
#   Tambahan anti-sideways: Efficiency Ratio (|net move| / total path) HTF_STRUCT_ER_BARS
#   candle terakhir harus >= HTF_STRUCT_ER_MIN (0 = tanpa syarat ER).
# Matikan/A-B lewat BOT_OVERRIDES di backtest.py: {"HTF_STRUCT_GATE_ON": False}
HTF_STRUCT_GATE_ON   = True
HTF_STRUCT_LOOKBACK  = 36     # candle HTF untuk range dasar
HTF_STRUCT_RECENT    = 18     # jendela cari breakout/breakdown terbaru
HTF_STRUCT_HOLD_ATR  = 0.5    # harga boleh turun sebanyak ini (x ATR) dari level breakout
HTF_STRUCT_PIVOT     = 3      # lebar pivot swing (candle kiri/kanan)
HTF_STRUCT_ER_BARS   = 24
HTF_STRUCT_ER_MIN    = 0.12


def check_htf_structure(df, direction, atr=None):
    """(ok, alasan, info). Struktur jelas di HTF: breakout/breakdown atau HH+HL / LH+LL
    dan bukan sideways (ER). Tanpa lookahead: pivot butuh HTF_STRUCT_PIVOT candle konfirmasi."""
    try:
        L, K, P = int(HTF_STRUCT_LOOKBACK), int(HTF_STRUCT_RECENT), int(HTF_STRUCT_PIVOT)
        if df is None or len(df) < L + K + 5:
            return True, "data HTF kurang — gate dilewati", {}
        hi = df["high"].astype(float).reset_index(drop=True)
        lo = df["low"].astype(float).reset_index(drop=True)
        cl = df["close"].astype(float).reset_index(drop=True)
        if not atr or atr <= 0:
            tr = (hi - lo).abs()
            atr = float(tr.iloc[-14:].mean())
        n = len(df)
        px = float(cl.iloc[-1])
        # Efficiency ratio (anti-sideways)
        er = 1.0
        eb = int(HTF_STRUCT_ER_BARS)
        if HTF_STRUCT_ER_MIN > 0 and n > eb + 1:
            path = float(cl.diff().abs().iloc[-eb:].sum())
            er = abs(px - float(cl.iloc[-eb - 1])) / path if path > 0 else 0.0
            if er < HTF_STRUCT_ER_MIN:
                return False, f"sideways (ER {er:.2f} < {HTF_STRUCT_ER_MIN:g})", {"er": er}
        # Range dasar = LOOKBACK candle sebelum jendela RECENT
        base_hi = float(hi.iloc[n - K - L:n - K].max())
        base_lo = float(lo.iloc[n - K - L:n - K].min())
        rc = cl.iloc[-K:]
        hold = HTF_STRUCT_HOLD_ATR * atr
        if direction == "BULLISH":
            brk = bool((rc > base_hi).any()) and px >= base_hi - hold
        else:
            brk = bool((rc < base_lo).any()) and px <= base_lo + hold
        # Pivot swing (terkonfirmasi) di ~80 candle terakhir
        w = 80
        s0 = max(P, n - w)
        sh, sl_ = [], []
        for i in range(s0, n - P):
            seg_h = hi.iloc[i - P:i + P + 1]
            seg_l = lo.iloc[i - P:i + P + 1]
            if hi.iloc[i] >= seg_h.max():
                sh.append(float(hi.iloc[i]))
            if lo.iloc[i] <= seg_l.min():
                sl_.append(float(lo.iloc[i]))
        trend = False
        if len(sh) >= 2 and len(sl_) >= 2:
            if direction == "BULLISH":
                trend = sh[-1] > sh[-2] and sl_[-1] > sl_[-2]
            else:
                trend = sh[-1] < sh[-2] and sl_[-1] < sl_[-2]
        info = {"er": er, "breakout": brk, "trend_struct": trend,
                "base_hi": base_hi, "base_lo": base_lo}
        if brk or trend:
            return True, ("breakout/breakdown HTF" if brk else "struktur HH+HL/LH+LL"), info
        return False, "tidak ada breakout & struktur HTF belum jelas (sideways)", info
    except Exception as e:  # noqa: BLE001
        return True, f"error {e} — gate dilewati", {}


ADX_RANGING_THRESHOLD  = 20   # ADX di bawah ini dianggap ranging/choppy

SCORE_ADX_RANGING_RSI_BONUS        = 6    # ranging + RSI extreme → mean-reversion masuk akal
SCORE_ADX_RANGING_MOMENTUM_PENALTY = -6   # ranging + momentum chase → rawan fakeout
SCORE_ADX_TRENDING_MOMENTUM_BONUS  = 6    # trending + momentum searah → breakout masuk akal

# ── LEGACY (TIDAK DIPAKAI): konstanta buffer SL ala calculate_rr_signalonly ──
# calculate_rr_signalonly() sudah dihapus saat porting ke algoritma main20.py;
# calculate_rr() sekarang murni versi main20 (buffer = atr * 0.5, satu nilai
# untuk semua sisi). Konstanta di bawah dibiarkan ada untuk referensi saja.
SL_BUF_TIGHT_ATR  = 0.35
SL_BUF_NORMAL_ATR = 0.60
SL_MAX_ATR_DIST   = 3.0


def _find_nearest_bullish_ob(df: pd.DataFrame, below_price: float) -> Optional[dict]:
    """Cari Bullish OB (demand zone) terdekat DI BAWAH harga saat ini. Untuk SL LONG."""
    start = max(1, len(df) - OB_LOOKBACK)
    candidates = []
    for i in range(start, len(df) - 2):
        c   = df.iloc[i]
        nxt = df.iloc[i + 1]
        if float(c["close"]) < float(c["open"]) and float(nxt["close"]) > float(nxt["open"]):
            impulse_pct = abs(float(nxt["close"]) - float(nxt["open"])) / (float(nxt["open"]) + 1e-9) * 100
            if impulse_pct < MIN_DISPLACEMENT_PCT:
                continue
            ob_high = float(c["high"])
            ob_low  = float(c["low"])
            ob_mid  = (ob_high + ob_low) / 2
            if ob_high < below_price:
                taps = sum(
                    1 for j in range(i + 2, len(df))
                    if float(df.iloc[j]["low"]) <= ob_high and float(df.iloc[j]["high"]) >= ob_low
                )
                if taps < OB_MITIGATION_LIMIT:
                    candidates.append({
                        "high": ob_high, "low": ob_low, "mid": ob_mid,
                        "dist": below_price - ob_high, "index": i
                    })
    if not candidates:
        return None
    return min(candidates, key=lambda x: x["dist"])


def _find_nearest_bearish_ob(df: pd.DataFrame, above_price: float) -> Optional[dict]:
    """Cari Bearish OB (supply zone) terdekat DI ATAS harga saat ini. Untuk SL SHORT."""
    start = max(1, len(df) - OB_LOOKBACK)
    candidates = []
    for i in range(start, len(df) - 2):
        c   = df.iloc[i]
        nxt = df.iloc[i + 1]
        if float(c["close"]) > float(c["open"]) and float(nxt["close"]) < float(nxt["open"]):
            impulse_pct = abs(float(nxt["close"]) - float(nxt["open"])) / (float(nxt["open"]) + 1e-9) * 100
            if impulse_pct < MIN_DISPLACEMENT_PCT:
                continue
            ob_high = float(c["high"])
            ob_low  = float(c["low"])
            ob_mid  = (ob_high + ob_low) / 2
            if ob_low > above_price:
                taps = sum(
                    1 for j in range(i + 2, len(df))
                    if float(df.iloc[j]["low"]) <= ob_high and float(df.iloc[j]["high"]) >= ob_low
                )
                if taps < OB_MITIGATION_LIMIT:
                    candidates.append({
                        "high": ob_high, "low": ob_low, "mid": ob_mid,
                        "dist": ob_low - above_price, "index": i
                    })
    if not candidates:
        return None
    return min(candidates, key=lambda x: x["dist"])


def _find_bullish_ob_target(df: pd.DataFrame, above_price: float) -> Optional[list]:
    """Cari Bullish OB DI BAWAH entry sebagai daftar target TP SHORT (dekat → jauh)."""
    start = max(1, len(df) - OB_LOOKBACK * 2)
    candidates = []
    for i in range(start, len(df) - 2):
        c   = df.iloc[i]
        nxt = df.iloc[i + 1]
        if float(c["close"]) < float(c["open"]) and float(nxt["close"]) > float(nxt["open"]):
            impulse_pct = abs(float(nxt["close"]) - float(nxt["open"])) / (float(nxt["open"]) + 1e-9) * 100
            if impulse_pct < MIN_DISPLACEMENT_PCT:
                continue
            ob_mid = (float(c["high"]) + float(c["low"])) / 2
            if ob_mid < above_price:
                candidates.append(ob_mid)
    if not candidates:
        return None
    return sorted(candidates, reverse=True)


def _find_bearish_ob_target(df: pd.DataFrame, below_price: float) -> Optional[list]:
    """Cari Bearish OB DI ATAS entry sebagai daftar target TP LONG (dekat → jauh)."""
    start = max(1, len(df) - OB_LOOKBACK * 2)
    candidates = []
    for i in range(start, len(df) - 2):
        c   = df.iloc[i]
        nxt = df.iloc[i + 1]
        if float(c["close"]) > float(c["open"]) and float(nxt["close"]) < float(nxt["open"]):
            impulse_pct = abs(float(nxt["close"]) - float(nxt["open"])) / (float(nxt["open"]) + 1e-9) * 100
            if impulse_pct < MIN_DISPLACEMENT_PCT:
                continue
            ob_mid = (float(c["high"]) + float(c["low"])) / 2
            if ob_mid > below_price:
                candidates.append(ob_mid)
    if not candidates:
        return None
    return sorted(candidates)


def _apply_sl_width_gate(result: tuple, symbol: str = "") -> Optional[tuple]:
    """
    Jaring terakhir: skip sinyal kalau SL width (entry→sl) melebihi
    SL_WIDTH_HARD_MAX_PCT dari entry — berlaku SEMUA mode, terlepas dari
    cap per-mode di SL_TP_CAPS (yang untuk LOW_TF/calculate_rr_signalonly
    sebelumnya tidak pernah benar-benar dicek).
    TIDAK clamp SL — hanya skip. SL harus tetap struktural, bukan direlokasi
    demi lolos filter ini.
    """
    if result is None:
        return None
    entry, sl, tp1, tp2, rr1, rr2, tp_anchored = result
    if entry:
        sl_width_pct = abs(entry - sl) / entry
        if sl_width_pct > SL_WIDTH_HARD_MAX_PCT:
            print(f"  ⏭️  [{symbol}] SL terlalu lebar ({sl_width_pct*100:.1f}% > "
                  f"{SL_WIDTH_HARD_MAX_PCT*100:.0f}%) — skip, invalidation struktural kejauhan")
            return None
    return result


def _swing_strength(level: float, df: pd.DataFrame, ob_list: list) -> float:
    """
    1:1 PORT dari base signal-only (main41).
    Skor "kekuatan" sebuah swing level, dipakai untuk memilih SL yang berbasis
    level struktural asli, bukan sekadar swing paling dekat secara jarak.

    Dua komponen:
      1. Overlap dengan Order Block — kalau level ini jatuh di dalam range OB
         (dengan toleransi SL_SWING_OB_TOLERANCE), warisi `strength` OB itu
         (find_order_blocks() sudah menghitung strength dari impulse%, jarak,
         dan jumlah tap).
      2. Rejection wick di candle yang membentuk swing itu — wick panjang
         relatif ke body (wick_body_ratio-style) menandakan penolakan harga
         yang tegas di level tsb, bukan cuma noise.

    Return 0.0 kalau level tidak overlap OB manapun dan tidak ada rejection
    wick signifikan (di-treat sebagai swing "lemah").
    """
    score = 0.0

    # Komponen 1: overlap dengan OB
    tol = level * SL_SWING_OB_TOLERANCE
    for ob_item in ob_list:
        if ob_item["low"] - tol <= level <= ob_item["high"] + tol:
            score += ob_item["strength"]
            break  # OB terkuat yang overlap sudah cukup (ob_list sudah tersortir desc)

    # Komponen 2: rejection wick di candle swing tsb (cari candle dengan high/low == level)
    matches = df[(df["high"] == level) | (df["low"] == level)]
    if not matches.empty:
        c = matches.iloc[-1]
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        body = abs(cl - o)
        if body > 1e-9:
            if h == level:
                wick_ratio = (h - max(o, cl)) / body
            else:
                wick_ratio = (min(o, cl) - l) / body
            if wick_ratio >= WICK_BODY_RATIO_MIN:
                score += wick_ratio

    return score


def _pick_sl_swing(candidates: list, entry: float, atr: float, df: pd.DataFrame, ob_list: list):
    """
    1:1 PORT dari base signal-only (main41).
    Pilih level SL dari daftar swing candidates [(index, level), ...] yang
    sudah difilter ke sisi yang benar (di bawah entry untuk BULLISH, di atas
    untuk BEARISH).

    Prioritas: swing dengan _swing_strength() tertinggi di antara SEMUA
    kandidat (jarak tidak membatasi penilaian — swing struktural yang kuat
    boleh saja lebih jauh dari swing noise terdekat). SL_MAX_ATR_MULT×ATR
    hanya diterapkan SETELAHNYA sebagai clamp, supaya swing terkuat tetap
    bisa terpilih walau jauh, tapi jarak SL akhir tidak pernah melebihi cap.
    Kalau tidak ada swing dengan strength > 0 sama sekali, fallback ke
    swing terdekat (perilaku lama).
    """
    if not candidates:
        return None

    max_dist = atr * SL_MAX_ATR_MULT

    scored = [(lvl, _swing_strength(lvl, df, ob_list)) for _, lvl in candidates]
    strong = [s for s in scored if s[1] > 0]

    if strong:
        # swing terkuat; kalau seri, pilih yang lebih dekat ke entry
        best_level = max(strong, key=lambda s: (s[1], -abs(s[0] - entry)))[0]
    else:
        # tidak ada swing kuat → fallback perilaku lama: swing terdekat
        best_level = min(candidates, key=lambda c: abs(c[1] - entry))[1]

    # Clamp jarak akhir ke cap, arah tetap ke sisi swing yang dipilih
    if abs(best_level - entry) > max_dist:
        best_level = entry - max_dist if best_level < entry else entry + max_dist

    return best_level


def calculate_rr(df, direction, ob, fvg) -> tuple:
    """1:1 PORT dari base signal-only (main41)."""
    market_price = float(df["close"].iloc[-1])
    atr          = calculate_atr(df)
    # (2026-09-03) Buffer dinaikkan 0.85 → 1.2×ATR. Backtest 64 trade RELAXED
    # menunjukkan 35/64 (55%) close SL — banyak yang plausible kena noise wick
    # tipis di sekitar level swing/OB/FVG, bukan reversal struktural asli.
    # Trade-off: RR sedikit turun (SL lebih jauh), tapi lebih tahan noise.
    buf          = atr * SL_BUFFER_ATR_MULT
    highs, lows  = find_swings(df)
    ob_list_all  = find_order_blocks(df, direction)

    if ob:      entry = ob["mid"]
    elif fvg:   entry = fvg["mid"]
    else:       entry = market_price

    if direction == "BULLISH":
        sl_candidates = []
        if ob:    sl_candidates.append(ob["low"] - buf)
        if fvg:   sl_candidates.append(fvg["bottom"] - buf)
        # FIX (2026-08-30): swing low terdekat HARUS di bawah entry untuk BULLISH.
        # Sebelumnya min(key=abs(x[1]-entry)) bisa memilih swing low yang justru
        # di ATAS entry (dianggap "terdekat" secara jarak absolut tanpa cek sisi),
        # bikin sl_candidates berisi nilai > entry → dist jadi negatif → TP
        # kebalik ke sisi yang salah. abs() di rr1/rr2 menyembunyikan ini karena
        # RR tetap kelihatan positif meski arahnya salah.
        # FIX (2026-09-02): dari swing yang valid (di bawah entry), pilih yang
        # PALING KUAT (overlap OB / rejection wick) dalam cap SL_MAX_ATR_MULT×ATR,
        # bukan sekadar yang jaraknya paling dekat — lihat _pick_sl_swing().
        lows_below = [lv for lv in lows if lv[1] < entry] if lows else []
        if lows_below:
            picked = _pick_sl_swing(lows_below, entry, atr, df, ob_list_all)
            if picked is not None:
                sl_candidates.append(picked - buf)
        sl   = min(sl_candidates) if sl_candidates else entry - atr * 2.0
        dist = entry - sl
        tp1  = entry + dist * RR_GRADE_B
        tp2  = entry + dist * (RR_GRADE_A + 0.5)
    else:
        sl_candidates = []
        if ob:    sl_candidates.append(ob["high"] + buf)
        if fvg:   sl_candidates.append(fvg["top"] + buf)
        # FIX (2026-08-30): sama seperti di atas — swing high terdekat harus di
        # ATAS entry untuk BEARISH, kalau tidak dist jadi negatif dan tp1/tp2
        # keluar di sisi yang salah (di atas entry, padahal SHORT harus TP di bawah).
        # FIX (2026-09-02): sama seperti BULLISH di atas, pilih swing high
        # TERKUAT dalam cap, bukan sekadar terdekat.
        highs_above = [hv for hv in highs if hv[1] > entry] if highs else []
        if highs_above:
            picked = _pick_sl_swing(highs_above, entry, atr, df, ob_list_all)
            if picked is not None:
                sl_candidates.append(picked + buf)
        sl   = max(sl_candidates) if sl_candidates else entry + atr * 2.0
        dist = sl - entry
        tp1  = entry - dist * RR_GRADE_B
        tp2  = entry - dist * (RR_GRADE_A + 0.5)

    # FIX (2026-08-30): guard terakhir — kalau dist tetap <= 0 (semua kandidat
    # habis atau tetap salah sisi), jangan hasilkan sinyal dengan TP terbalik.
    # Tandai lewat rr1/rr2 = 0.0 sehingga grade_signal() di caller menolaknya
    # (grade_signal butuh rr1 >= RR_GRADE_B > 0).
    if dist <= 0:
        return entry, sl, tp1, tp2, 0.0, 0.0

    if SINGLE_TP_MODE:
        tp1 = entry + dist * SINGLE_TP_R_MULT if direction == "BULLISH" else entry - dist * SINGLE_TP_R_MULT
        tp2 = tp1

    sl_dist = abs(entry - sl)
    rr1     = round(abs(tp1 - entry) / sl_dist, 2) if sl_dist > 0 else 0.0
    rr2     = round(abs(tp2 - entry) / sl_dist, 2) if sl_dist > 0 else 0.0
    return entry, sl, tp1, tp2, rr1, rr2



# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 14 — SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def compute_score(
    # ── Tier 1: HTF ──────────────────────────────────────────
    htf_aligned: bool,
    # ── Tier 2: BOS / Market Structure ───────────────────────
    bos_confirmed: bool,
    bos_strength: str,          # "STRONG" | "WEAK" | "NONE"
    # ── Tier 3: Momentum ──────────────────────────────────────
    momentum_aligned: bool,     # N candle berturut arah sama
    momentum_present: bool,     # ada momentum, meski campuran
    # ── Tier 4: Liquidity ─────────────────────────────────────
    liq_swept: bool,
    sweep_strength: str,        # "STRONG" | "RELAXED" | None
    # ── Tier 5: Entry Zone ────────────────────────────────────
    in_ob: bool,
    in_fvg: bool,
    # ── Supporting factors ────────────────────────────────────
    displacement: bool,
    vol_rat: float,
    session: str,
    rsi: float,
    macro_pts: int,
    ema_pts: int,
    ref_aligned: bool,
    adx: float = 25.0,
) -> tuple:
    """
    PORTED DARI main20.py (SIGNAL-ONLY reference bot) — menggantikan versi
    D&S Zone edition yang sebelumnya ada di sini.

    Scoring-based engine mengikuti SMC flow:
      HTF bias → Liquidity target → Sweep/reaksi → BOS/momentum → Entry OB/FVG

    Tidak ada hard-gate; semua elemen berkontribusi ke total score.
    Sinyal dikirim jika total ≥ threshold.
    """
    bd = {}

    # ── Tier 1: HTF (−10 … +15) ──────────────────────────────────────────────
    bd["htf"] = SCORE_HTF_ALIGNED if htf_aligned else SCORE_HTF_PENALTY

    # ── Tier 2: BOS (−10 … +25) ──────────────────────────────────────────────
    if bos_confirmed:
        bd["bos"] = SCORE_BOS_STRONG if bos_strength == "STRONG" else SCORE_BOS_WEAK
    else:
        bd["bos"] = SCORE_BOS_PENALTY

    # ── Tier 3: Momentum (−5 … +15) ──────────────────────────────────────────
    # FIX (2026-08-31, 1:1 PORT dari base): dibalik berdasarkan backtest 444
    # trade — momentum_aligned justru WR paling jelek (25.5%, -16.4R, n=102)
    # vs tanpa-momentum-searah yang WR 36% (+42.3R, n=247). SCORE_MOMENTUM/
    # SCORE_MOMENTUM_ANTI ditukar.
    if momentum_aligned:
        bd["momentum"] = SCORE_MOMENTUM_ANTI
    elif momentum_present:
        bd["momentum"] = SCORE_MOMENTUM_NONE
    else:
        bd["momentum"] = SCORE_MOMENTUM

    # ── Tier 4: Liquidity sweep (−5 … +15) ───────────────────────────────────
    if liq_swept and sweep_strength == "STRONG":
        bd["liquidity"] = SCORE_LIQUIDITY
    elif liq_swept:
        bd["liquidity"] = SCORE_LIQUIDITY_WEAK
    else:
        bd["liquidity"] = SCORE_LIQUIDITY_NONE

    # ── Tier 5: Entry zone OB/FVG (0 … +15) ──────────────────────────────────
    if in_ob and in_fvg:
        bd["entry_zone"] = SCORE_OB_AND_FVG
    elif in_ob or in_fvg:
        bd["entry_zone"] = SCORE_OB_OR_FVG
    else:
        bd["entry_zone"] = SCORE_ZONE_NONE

    # ── Supporting ────────────────────────────────────────────────────────────
    bd["displacement"] = SCORE_DISPLACEMENT if displacement else 0
    bd["volume"]       = SCORE_VOLUME if vol_rat >= 1.5 else (2 if vol_rat >= 1.2 else 0)
    bd["session"]      = session_score(session)
    bd["rsi"]          = rsi_score(rsi)
    # 1:1 PARITY FIX: regime_score (ADX) dihapus dari total skor — komponen
    # ini tidak ada di signal-only base (signal.py). ADX tetap dihitung di
    # analyze_pair() untuk ditampilkan di reasons/log, tapi TIDAK lagi
    # berkontribusi ke keputusan skor/threshold, supaya output sinyal
    # (GO/NO-GO dan grade) identik dengan signal-only.
    bd["macro"]        = macro_pts
    bd["ema"]          = ema_pts
    bd["ref_tf"]       = SCORE_REF_TF if ref_aligned else 0

    total = sum(bd.values())
    return total, bd


def grade_signal(rr: float) -> Optional[str]:
    if rr >= RR_GRADE_A:  return "A"
    if rr >= RR_GRADE_B:  return "B"
    return None


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 15 — VOLATILITY & CONFIRMATION
# ═══════════════════════════════════════════════════════════════════════════

def check_volatility(df: pd.DataFrame) -> tuple:
    atr     = calculate_atr(df)
    price   = float(df["close"].iloc[-1])
    atr_pct = (atr / price) * 100 if price > 0 else 0.0

    if atr_pct < ATR_MIN_PCT:
        return False, f"Low volatility: ATR={atr_pct:.3f}%"

    recent    = df.iloc[-10:]
    r_high    = float(recent["high"].max())
    r_low     = float(recent["low"].min())
    range_pct = ((r_high - r_low) / r_low * 100) if r_low > 0 else 0.0
    if range_pct < ATR_MIN_PCT * 3:
        return False, f"Ranging: 10-candle range={range_pct:.3f}%"

    return True, f"Volatility OK: ATR={atr_pct:.3f}%"


def check_confirmation_candle(df: pd.DataFrame, direction: str) -> bool:
    c    = df.iloc[-1]
    prev = df.iloc[-2]
    o,  h,  l,  cl  = float(c["open"]),    float(c["high"]),    float(c["low"]),    float(c["close"])
    po, ph, pl, pcl = float(prev["open"]), float(prev["high"]), float(prev["low"]), float(prev["close"])

    candle_range = h - l
    if candle_range < 1e-9:
        return False

    body_ratio = abs(cl - o) / candle_range
    if direction == "BULLISH":
        return (cl > o and body_ratio >= CONFIRM_BODY_RATIO) or (cl > o and cl > po and o < pcl)
    else:
        return (cl < o and body_ratio >= CONFIRM_BODY_RATIO) or (cl < o and cl < po and o > pcl)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 15b — DEMAND & SUPPLY ZONE ENGINE (ported from SIGNALONLY)
# ═══════════════════════════════════════════════════════════════════════════

# ── D&S Zone Config ─────────────────────────────────────────────────────────
DS_LOOKBACK          = 60    # candle lookback untuk cari zona D&S di HTF
DS_IMPULSE_MIN_PCT   = 0.30  # impulse setelah zona minimal 0.3%
DS_ZONE_TAPS_MAX     = 2     # zona yang sudah >2x disentuh = mitigated
DS_ZONE_TOLERANCE    = 0.005 # ±0.5% tolerance untuk "harga di dalam zona"
DS_BOS_CONFIRM_PCT   = 0.003 # BOS valid jika close melewati swing ≥0.3%
DS_FRESHNESS_WEIGHT  = 0.4
DS_IMPULSE_WEIGHT    = 0.6

# ── Rejection Candle Config ──────────────────────────────────────────────────
REJECTION_WICK_RATIO    = 1.8
REJECTION_BODY_MIN      = 0.30
REJECTION_CLOSE_PCT     = 0.60
# SNIPER FIX: diturunkan dari 3 → 2. Rejection candle yang sudah 3 bar lalu
# dianggap "basi" untuk entry presisi — harga sudah sempat bergerak menjauh
# dari titik konfirmasi saat order akhirnya dieksekusi. Prioritaskan candle
# -1 (paling baru), toleransi maksimal -2.
REJECTION_LOOKBACK      = 3   # FIX: disamakan dengan SIGNALONLY (base). Sebelumnya 2,
# menyebabkan hard-gate "wajib rejection candle" di analyze_pair() jadi lebih ketat
# di AUTOTRADE dibanding SIGNALONLY, walaupun kode gate-nya sendiri sudah identik.


def detect_demand_supply_zones(df, direction):
    zones  = []
    price  = float(df["close"].iloc[-1])
    start  = max(2, len(df) - DS_LOOKBACK)
    for i in range(start, len(df) - 2):
        base = df.iloc[i]; nxt = df.iloc[i + 1]
        o_b, h_b, l_b, c_b = float(base["open"]), float(base["high"]), float(base["low"]), float(base["close"])
        o_n, h_n, l_n, c_n = float(nxt["open"]),  float(nxt["high"]),  float(nxt["low"]),  float(nxt["close"])
        impulse_pct = abs(c_n - o_n) / (o_n + 1e-9) * 100
        if impulse_pct < DS_IMPULSE_MIN_PCT:
            continue
        zone_type = None
        if direction == "BULLISH":
            if c_b <= o_b and c_n > o_n and c_n > h_b:
                zone_type = "DEMAND"
        elif direction == "BEARISH":
            if c_b >= o_b and c_n < o_n and c_n < l_b:
                zone_type = "SUPPLY"
        if zone_type is None:
            continue
        z_high = h_b; z_low = l_b; z_mid = (z_high + z_low) / 2
        taps = 0; mitigated = False
        for j in range(i + 2, len(df)):
            cj = df.iloc[j]
            lo_j, hi_j = float(cj["low"]), float(cj["high"])
            if lo_j <= z_high and hi_j >= z_low:
                taps += 1
                cl_j = float(cj["close"])
                if direction == "BULLISH" and cl_j < z_low: mitigated = True; break
                if direction == "BEARISH" and cl_j > z_high: mitigated = True; break
        if mitigated or taps > DS_ZONE_TAPS_MAX:
            continue
        if direction == "BULLISH" and price < z_low * 0.97: continue
        if direction == "BEARISH" and price > z_high * 1.03: continue
        freshness = 1.0 / (1.0 + taps)
        impulse_n = min(1.0, impulse_pct / 2.0)
        strength  = DS_IMPULSE_WEIGHT * impulse_n + DS_FRESHNESS_WEIGHT * freshness
        zones.append({"low": z_low, "high": z_high, "mid": z_mid,
                      "strength": strength, "taps": taps, "impulse_pct": impulse_pct,
                      "index": i, "mitigated": mitigated, "zone_type": zone_type})
    zones.sort(key=lambda x: x["strength"], reverse=True)
    return zones[:5]


def price_in_ds_zone(price, zones, tolerance=DS_ZONE_TOLERANCE):
    if not zones: return False, None
    lo = price * (1 - tolerance); hi = price * (1 + tolerance)
    hits = [z for z in zones if z["low"] <= hi and z["high"] >= lo]
    if not hits: return False, None
    return True, max(hits, key=lambda x: x["strength"])


def check_bos_to_zone(df, direction, zones):
    if not zones: return False, False, None, None, "Tidak ada zona D&S terdeteksi"
    highs, lows = find_swings(df)
    price = float(df["close"].iloc[-1])
    if direction == "BULLISH":
        if not highs: return False, False, None, None, "Tidak ada swing high"
        last_sh = highs[-1][1]; bos_confirmed = False; bos_level = last_sh
        for i in range(-10, 0):
            try:
                if float(df.iloc[i]["close"]) > last_sh * (1 + DS_BOS_CONFIRM_PCT):
                    bos_confirmed = True; break
            except Exception: pass
        if not bos_confirmed:
            return False, False, None, None, f"Belum ada BOS bullish (swing high {last_sh:.4f})"
        in_zone, best_zone = price_in_ds_zone(price, zones)
        if in_zone and best_zone:
            return True, True, best_zone, bos_level, (
                f"BOS bullish up melewati {last_sh:.4f} -> pullback ke Demand Zone "
                f"[{best_zone['low']:.4f}-{best_zone['high']:.4f}] "
                f"(strength {best_zone['strength']:.2f}, taps {best_zone['taps']})")
        return True, False, None, bos_level, "BOS bullish terkonfirmasi - menunggu pullback ke Demand Zone"
    else:
        if not lows: return False, False, None, None, "Tidak ada swing low"
        last_sl = lows[-1][1]; bos_confirmed = False; bos_level = last_sl
        for i in range(-10, 0):
            try:
                if float(df.iloc[i]["close"]) < last_sl * (1 - DS_BOS_CONFIRM_PCT):
                    bos_confirmed = True; break
            except Exception: pass
        if not bos_confirmed:
            return False, False, None, None, f"Belum ada BOS bearish (swing low {last_sl:.4f})"
        in_zone, best_zone = price_in_ds_zone(price, zones)
        if in_zone and best_zone:
            return True, True, best_zone, bos_level, (
                f"BOS bearish down melewati {last_sl:.4f} -> pullback ke Supply Zone "
                f"[{best_zone['low']:.4f}-{best_zone['high']:.4f}] "
                f"(strength {best_zone['strength']:.2f}, taps {best_zone['taps']})")
        return True, False, None, bos_level, "BOS bearish terkonfirmasi - menunggu pullback ke Supply Zone"


def detect_rejection_candle(df, direction, ob=None, fvg=None):
    if len(df) < 4: return False, "NONE", 0.0, "Data tidak cukup"
    best_pattern = "NONE"; best_strength = 0.0; best_reason = ""
    for offset in range(1, REJECTION_LOOKBACK + 1):
        if len(df) < offset + 2: break
        c = df.iloc[-offset]; prev = df.iloc[-(offset + 1)]
        o, h, l, cl   = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        po, ph, pl, pcl = float(prev["open"]), float(prev["high"]), float(prev["low"]), float(prev["close"])
        candle_range = h - l
        if candle_range < 1e-9: continue
        body = abs(cl - o); body_pct = body / candle_range
        upper_wick = h - max(o, cl); lower_wick = min(o, cl) - l
        in_zone = True
        if ob:   in_zone = (l <= ob["high"] and h >= ob["low"])
        elif fvg:
            ft = fvg.get("top", fvg.get("high", h)); fb = fvg.get("bottom", fvg.get("low", l))
            in_zone = (l <= ft and h >= fb)
        if not in_zone and (ob or fvg): continue
        if direction == "BULLISH":
            close_pct = (cl - l) / candle_range
            if (lower_wick > 0 and body > 1e-9 and lower_wick / body >= REJECTION_WICK_RATIO
                    and body_pct >= REJECTION_BODY_MIN and close_pct >= REJECTION_CLOSE_PCT and cl > o):
                strength = min(1.0, (lower_wick / body) / 4.0 * close_pct)
                if strength > best_strength:
                    best_pattern = "PIN_BAR"; best_strength = strength
                    best_reason = f"Hammer: lower wick {lower_wick/body:.1f}x body, close {close_pct*100:.0f}% dari low"
            prev_bearish = pcl < po
            if (prev_bearish and cl > po and o < pcl and cl > o and body_pct >= 0.50):
                strength = min(1.0, (body / (abs(pcl - po) + 1e-9)) * body_pct)
                if strength > best_strength:
                    best_pattern = "ENGULFING"; best_strength = strength
                    best_reason = f"Bullish Engulfing: body {body_pct*100:.0f}% range"
            # ── FIX GAP: INSIDE_BREAK — hilang di versi lama, port dari SIGNALONLY ──
            # Candle sekarang "inside" candle sebelumnya (mother bar), lalu candle
            # berikutnya breakout ke atas dengan body solid → momentum reversal.
            is_inside = (h <= ph and l >= pl)
            if is_inside and not prev_bearish and offset > 1:
                next_c = df.iloc[-(offset - 1)]
                no, nh, nl, ncl = float(next_c["open"]), float(next_c["high"]), float(next_c["low"]), float(next_c["close"])
                if ncl > o and (ncl - no) / (nh - nl + 1e-9) >= 0.50:
                    strength = 0.65
                    if strength > best_strength:
                        best_pattern = "INSIDE_BREAK"; best_strength = strength
                        best_reason = f"Inside Bar Breakout bullish @ candle -{offset}"
        else:
            close_pct_bear = (h - cl) / candle_range
            if (upper_wick > 0 and body > 1e-9 and upper_wick / body >= REJECTION_WICK_RATIO
                    and body_pct >= REJECTION_BODY_MIN and close_pct_bear >= REJECTION_CLOSE_PCT and cl < o):
                strength = min(1.0, (upper_wick / body) / 4.0 * close_pct_bear)
                if strength > best_strength:
                    best_pattern = "PIN_BAR"; best_strength = strength
                    best_reason = f"Shooting Star: upper wick {upper_wick/body:.1f}x body, close {close_pct_bear*100:.0f}% dari high"
            prev_bullish = pcl > po
            if (prev_bullish and cl < po and o > pcl and cl < o and body_pct >= 0.50):
                strength = min(1.0, (body / (abs(pcl - po) + 1e-9)) * body_pct)
                if strength > best_strength:
                    best_pattern = "ENGULFING"; best_strength = strength
                    best_reason = f"Bearish Engulfing: body {body_pct*100:.0f}% range"
            # ── FIX GAP: INSIDE_BREAK — hilang di versi lama, port dari SIGNALONLY ──
            is_inside = (h <= ph and l >= pl)
            if is_inside and not prev_bullish and offset > 1:
                next_c = df.iloc[-(offset - 1)]
                no, nh, nl, ncl = float(next_c["open"]), float(next_c["high"]), float(next_c["low"]), float(next_c["close"])
                if ncl < no and (no - ncl) / (nh - nl + 1e-9) >= 0.50:
                    strength = 0.65
                    if strength > best_strength:
                        best_pattern = "INSIDE_BREAK"; best_strength = strength
                        best_reason = f"Inside Bar Breakout bearish @ candle -{offset}"
    confirmed = best_pattern != "NONE" and best_strength >= 0.35
    return confirmed, best_pattern, best_strength, best_reason


def stoch_zone_label(k: float) -> str:
    if k < STOCH_OVERSOLD:
        return "OVERSOLD"
    if k > STOCH_OVERBOUGHT:
        return "OVERBOUGHT"
    return "NEUTRAL"



def calculate_stochastic(df: pd.DataFrame,
                          k_period: int = STOCH_K_PERIOD,
                          d_period: int = STOCH_D_PERIOD,
                          smooth: int   = STOCH_SMOOTH) -> dict:
    """
    Hitung Stochastic Slow (5,3,3).
    Return dict:
      k        : nilai %K terbaru (0-100)
      d        : nilai %D terbaru (0-100)
      k_prev   : %K candle sebelumnya
      d_prev   : %D candle sebelumnya
      k_series : list semua nilai %K (untuk divergence)
      d_series : list semua nilai %D
    """
    if len(df) < k_period + d_period + smooth + 5:
        return {"k": 50.0, "d": 50.0, "k_prev": 50.0, "d_prev": 50.0,
                "k_series": [], "d_series": []}

    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values

    # Fast %K
    fast_k = []
    for i in range(k_period - 1, len(closes)):
        hh = max(highs[i - k_period + 1: i + 1])
        ll = min(lows[i  - k_period + 1: i + 1])
        if hh == ll:
            fast_k.append(50.0)
        else:
            fast_k.append(100.0 * (closes[i] - ll) / (hh - ll))

    # Slow %K = SMA(fast_k, smooth)
    slow_k = []
    for i in range(smooth - 1, len(fast_k)):
        slow_k.append(sum(fast_k[i - smooth + 1: i + 1]) / smooth)

    # Slow %D = SMA(slow_k, d_period)
    slow_d = []
    for i in range(d_period - 1, len(slow_k)):
        slow_d.append(sum(slow_k[i - d_period + 1: i + 1]) / d_period)

    if not slow_k or not slow_d:
        return {"k": 50.0, "d": 50.0, "k_prev": 50.0, "d_prev": 50.0,
                "k_series": [], "d_series": []}

    return {
        "k":        round(slow_k[-1], 2),
        "d":        round(slow_d[-1], 2),
        "k_prev":   round(slow_k[-2], 2) if len(slow_k) >= 2 else slow_k[-1],
        "d_prev":   round(slow_d[-2], 2) if len(slow_d) >= 2 else slow_d[-1],
        "k_series": slow_k,
        "d_series": slow_d,
    }


def detect_stoch_divergence(df: pd.DataFrame, stoch: dict,
                             direction: str, lookback: int = 20) -> tuple:
    """
    Deteksi divergence bullish atau bearish antara harga dan Stochastic.

    Bullish Divergence (BULLISH):
      Harga bikin LL (lower low) tapi Stoch K bikin HL (higher low)
      → momentum melemah, potensi reversal naik

    Bearish Divergence (BEARISH):
      Harga bikin HH (higher high) tapi Stoch K bikin LH (lower high)
      → momentum melemah, potensi reversal turun

    Return: (divergence_found: bool, divergence_type: str, reason: str)
    """
    k_series = stoch.get("k_series", [])
    if len(k_series) < lookback or len(df) < lookback:
        return False, "NONE", "Data tidak cukup"

    closes = df["close"].values[-lookback:]
    k_vals = k_series[-lookback:]

    if direction == "BULLISH":
        # Cari LL di harga dalam 2/3 awal window
        mid = len(closes) // 2
        # Cari 2 swing low harga
        price_lows = []
        for i in range(2, len(closes) - 1):
            if closes[i] < closes[i - 1] and closes[i] < closes[i + 1]:
                price_lows.append((i, closes[i]))

        k_lows = []
        for i in range(2, len(k_vals) - 1):
            if k_vals[i] < k_vals[i - 1] and k_vals[i] < k_vals[i + 1]:
                k_lows.append((i, k_vals[i]))

        if len(price_lows) >= 2 and len(k_lows) >= 2:
            # Ambil 2 low terakhir
            pl1, pl2 = price_lows[-2], price_lows[-1]
            kl1, kl2 = k_lows[-2],    k_lows[-1]
            # Divergence: harga LL (pl2 < pl1) tapi K HL (kl2 > kl1)
            if pl2[1] < pl1[1] and kl2[1] > kl1[1]:
                reason = (f"📈 Bullish Divergence: Harga LL ({pl1[1]:.4f}→{pl2[1]:.4f}) "
                          f"tapi K HL ({kl1[1]:.1f}→{kl2[1]:.1f})")
                return True, "BULLISH_DIV", reason

    elif direction == "BEARISH":
        # Cari HH di harga tapi LH di Stoch
        price_highs = []
        for i in range(2, len(closes) - 1):
            if closes[i] > closes[i - 1] and closes[i] > closes[i + 1]:
                price_highs.append((i, closes[i]))

        k_highs = []
        for i in range(2, len(k_vals) - 1):
            if k_vals[i] > k_vals[i - 1] and k_vals[i] > k_vals[i + 1]:
                k_highs.append((i, k_vals[i]))

        if len(price_highs) >= 2 and len(k_highs) >= 2:
            ph1, ph2 = price_highs[-2], price_highs[-1]
            kh1, kh2 = k_highs[-2],    k_highs[-1]
            # Divergence: harga HH (ph2 > ph1) tapi K LH (kh2 < kh1)
            if ph2[1] > ph1[1] and kh2[1] < kh1[1]:
                reason = (f"📉 Bearish Divergence: Harga HH ({ph1[1]:.4f}→{ph2[1]:.4f}) "
                          f"tapi K LH ({kh1[1]:.1f}→{kh2[1]:.1f})")
                return True, "BEARISH_DIV", reason

    return False, "NONE", "Tidak ada divergence"


def detect_stoch_weakening(stoch: dict, direction: str) -> tuple:
    """
    Pelemahan momentum di Stochastic searah trade.
    SHORT : dalam STOCH_LOOKBACK candle terakhir %K sempat >= STOCH_OVERBOUGHT,
            sekarang %K < %D dan %K turun dari candle sebelumnya.
    LONG  : kebalikannya (sempat <= STOCH_OVERSOLD, sekarang %K > %D dan naik).
    Returns: (ok: bool, alasan: str)
    """
    try:
        ks = stoch.get("k_series", [])
        if len(ks) < STOCH_LOOKBACK + 2:
            return False, "data stoch kurang"
        k, d, kp = stoch["k"], stoch["d"], stoch["k_prev"]
        recent = ks[-(STOCH_LOOKBACK + 1):-1]
        if direction == "BEARISH":
            ok = max(recent) >= STOCH_OVERBOUGHT and k < d and k < kp
            return ok, (f"K sempat {max(recent):.0f} (OB) lalu turun, K {k:.0f} < D {d:.0f}" if ok
                        else f"belum melemah dari OB (maxK {max(recent):.0f}, K {k:.0f}, D {d:.0f})")
        ok = min(recent) <= STOCH_OVERSOLD and k > d and k > kp
        return ok, (f"K sempat {min(recent):.0f} (OS) lalu naik, K {k:.0f} > D {d:.0f}" if ok
                    else f"belum menguat dari OS (minK {min(recent):.0f}, K {k:.0f}, D {d:.0f})")
    except Exception as e:
        return False, f"error stoch: {e}"


def stoch_opposing_reversal(stoch: dict, trade_direction: str) -> tuple:
    """
    Cek apakah Stochastic sudah mentok di zona ekstrem BERLAWANAN dengan arah
    sinyal/posisi dan sedang mulai crossing balik ke arah lawan itu.

    Contoh: sinyal/posisi LONG, tapi Stochastic sudah OVERBOUGHT dan K mulai
    turun mendekati/menembus D (momentum mau balik turun) → dianggap
    berlawanan, sebaiknya skip dulu daripada entry melawan momentum jangka
    pendek yang baru mau berbalik.

    "Mau crossing" didefinisikan sebagai: K masih di atas D tapi jarak
    keduanya menyempit dibanding candle sebelumnya (momentum melemah), ATAU
    K sudah crossing melewati D ke arah lawan.

    Return: (opposing: bool, reason: str)
    """
    k      = stoch.get("k")
    d      = stoch.get("d")
    k_prev = stoch.get("k_prev")
    d_prev = stoch.get("d_prev")
    if k is None or d is None or k_prev is None or d_prev is None:
        return False, ""

    if trade_direction == "BULLISH":
        # Sinyal/posisi LONG — waspada kalau OVERBOUGHT dan mau cross turun
        if k < STOCH_OVERBOUGHT:
            return False, ""
        gap_prev = k_prev - d_prev
        gap_now  = k - d
        crossed_down   = k_prev >= d_prev and k < d
        narrowing_down = gap_now < gap_prev and gap_now < gap_prev * 0.9
        if crossed_down or narrowing_down:
            return True, (f"Stoch OVERBOUGHT (K={k:.1f}) dan mulai crossing turun "
                           f"(K:{k_prev:.1f}→{k:.1f}, D:{d_prev:.1f}→{d:.1f}) — berlawanan dengan LONG")
        return False, ""

    if trade_direction == "BEARISH":
        # Sinyal/posisi SHORT — waspada kalau OVERSOLD dan mau cross naik
        if k > STOCH_OVERSOLD:
            return False, ""
        gap_prev = d_prev - k_prev
        gap_now  = d - k
        crossed_up   = k_prev <= d_prev and k > d
        narrowing_up = gap_now < gap_prev and gap_now < gap_prev * 0.9
        if crossed_up or narrowing_up:
            return True, (f"Stoch OVERSOLD (K={k:.1f}) dan mulai crossing naik "
                           f"(K:{k_prev:.1f}→{k:.1f}, D:{d_prev:.1f}→{d:.1f}) — berlawanan dengan SHORT")
        return False, ""

    return False, ""


# ── BTC Setup Guard cache (1:1 reference bot) ───────────────────────────────
_btc_setup_cache: dict = {"data": None, "ts": 0.0}
BTC_SETUP_CACHE_TTL = 60   # refresh setiap 1 menit

# ── Cache df BTC HTF terpisah — dipakai gate divergence Stochastic supaya
# tidak fetch ulang ke Binance per pair (fetch_btc_setup_data() sudah fetch
# df BTC HTF secara internal tapi tidak expose df-nya, cuma hasil olahannya).
_btc_htf_df_cache: dict = {"df": None, "ts": 0.0}


def get_cached_btc_htf_df():
    """Ambil df BTC HTF (dipakai untuk cek Stoch divergence BTC), cache 60 detik."""
    global _btc_htf_df_cache
    now = time.time()
    if (_btc_htf_df_cache["df"] is not None
            and (now - _btc_htf_df_cache["ts"]) < BTC_SETUP_CACHE_TTL):
        return _btc_htf_df_cache["df"]
    try:
        df = fetch_ohlcv_safe("BTC/USDT", BTC_SETUP_TF, limit=BTC_SUPPLY_LOOKBACK + 50)
        _btc_htf_df_cache = {"df": df, "ts": now}
        return df
    except Exception:
        return _btc_htf_df_cache["df"]  # fallback ke cache lama kalau fetch gagal


def fetch_btc_setup_data() -> dict:
    """
    Fetch dan analisis setup BTC untuk digunakan sebagai filter global.
    Cache 60 detik agar tidak re-fetch per pair.

    Return dict:
      htf_bias        : 'BULLISH' / 'BEARISH' / 'RANGING'
      stoch_htf       : stochastic HTF BTC
      stoch_ltf       : stochastic LTF BTC
      in_supply       : harga BTC saat ini di zona supply HTF
      in_demand       : harga BTC saat ini di zona demand HTF
      reversal_signal : 'BULLISH' / 'BEARISH' / 'NONE' (konfirmasi reversal)
      reversal_reason : alasan konfirmasi
      block_long      : True jika LONG altcoin harus diblok
      block_short     : True jika SHORT altcoin harus diblok
      block_reason    : alasan blok
      ambiguous       : True jika BTC belum jelas arahnya
      setup_label     : label ringkas untuk log/signal
    """
    global _btc_setup_cache
    now = time.time()
    if (_btc_setup_cache["data"] is not None
            and (now - _btc_setup_cache["ts"]) < BTC_SETUP_CACHE_TTL):
        return _btc_setup_cache["data"]

    result = {
        "htf_bias": "RANGING", "stoch_htf": {}, "stoch_ltf": {},
        "in_supply": False, "in_demand": False,
        "reversal_signal": "NONE", "reversal_reason": "Data tidak tersedia",
        "block_long": False, "block_short": False, "block_reason": "",
        "ambiguous": True, "setup_label": "BTC:❓",
    }

    try:
        # ── Fetch data BTC HTF & LTF ─────────────────────────────────────────
        df_htf = fetch_ohlcv_safe("BTC/USDT", BTC_SETUP_TF, limit=BTC_SUPPLY_LOOKBACK + 50)
        time.sleep(0.05)
        df_ltf = fetch_ohlcv_safe("BTC/USDT", BTC_SETUP_LTF, limit=100)
        time.sleep(0.05)

        # ── HTF Bias & Struktur ──────────────────────────────────────────────
        htf_bias, _, _, _ = detect_structure(df_htf)
        result["htf_bias"] = htf_bias

        # ── Stochastic HTF & LTF ─────────────────────────────────────────────
        stoch_htf = calculate_stochastic(df_htf)
        stoch_ltf = calculate_stochastic(df_ltf)
        result["stoch_htf"] = stoch_htf
        result["stoch_ltf"] = stoch_ltf

        current_price = float(df_htf["close"].iloc[-1])

        # ── Supply / Demand Zone Detection di HTF BTC ─────────────────────────
        # Supply = OB bearish HTF (zona jual smart money)
        # Demand = OB bullish HTF (zona beli smart money)
        supply_zones = find_order_blocks(df_htf, "BEARISH")
        demand_zones = find_order_blocks(df_htf, "BULLISH")

        # Cek juga FVG sebagai tambahan zona
        fvg_supply = find_fvg(df_htf, "BEARISH")
        fvg_demand = find_fvg(df_htf, "BULLISH")

        in_supply_ob, _ = price_in_ob(current_price, supply_zones)
        in_demand_ob, _ = price_in_ob(current_price, demand_zones)
        in_supply_fvg   = price_in_fvg(current_price, fvg_supply)
        in_demand_fvg   = price_in_fvg(current_price, fvg_demand)

        in_supply = in_supply_ob or in_supply_fvg
        in_demand = in_demand_ob or in_demand_fvg
        result["in_supply"] = in_supply
        result["in_demand"] = in_demand

        # ── Stoch HTF Zone ───────────────────────────────────────────────────
        k_htf  = stoch_htf.get("k", 50)
        d_htf  = stoch_htf.get("d", 50)
        kp_htf = stoch_htf.get("k_prev", 50)

        k_ltf  = stoch_ltf.get("k", 50)
        d_ltf  = stoch_ltf.get("d", 50)
        kp_ltf = stoch_ltf.get("k_prev", 50)

        # ── Reversal Konfirmasi BTC ──────────────────────────────────────────
        # Bullish reversal: BTC di demand / oversold → K cross dari bawah ke atas
        # Bearish reversal: BTC di supply / overbought → K cross dari atas ke bawah
        reversal = "NONE"
        rev_reasons = []

        # Konfirmasi BULLISH reversal BTC
        bullish_stoch_cross = (kp_ltf < d_ltf and k_ltf > d_ltf) or \
                              (kp_htf < STOCH_OVERSOLD and k_htf >= STOCH_OVERSOLD)
        bullish_stoch_exit  = kp_htf < STOCH_REVERSAL_ZONE and k_htf >= STOCH_REVERSAL_ZONE
        bullish_zone_ok     = in_demand or htf_bias == "BULLISH"

        if bullish_stoch_cross and bullish_zone_ok:
            reversal = "BULLISH"
            rev_reasons.append(f"Stoch K cross bullish (K:{kp_ltf:.1f}→{k_ltf:.1f}, D:{d_ltf:.1f})")
        elif bullish_stoch_exit and htf_bias == "BULLISH":
            reversal = "BULLISH"
            rev_reasons.append(f"Stoch exit oversold (K:{kp_htf:.1f}→{k_htf:.1f})")

        # Konfirmasi BEARISH reversal BTC
        bearish_stoch_cross = (kp_ltf > d_ltf and k_ltf < d_ltf) or \
                              (kp_htf > STOCH_OVERBOUGHT and k_htf <= STOCH_OVERBOUGHT)
        bearish_stoch_exit  = kp_htf > (100 - STOCH_REVERSAL_ZONE) and k_htf <= (100 - STOCH_REVERSAL_ZONE)
        bearish_zone_ok     = in_supply or htf_bias == "BEARISH"

        if bearish_stoch_cross and bearish_zone_ok:
            reversal = "BEARISH"
            rev_reasons.append(f"Stoch K cross bearish (K:{kp_ltf:.1f}→{k_ltf:.1f}, D:{d_ltf:.1f})")
        elif bearish_stoch_exit and htf_bias == "BEARISH":
            reversal = "BEARISH"
            rev_reasons.append(f"Stoch exit overbought (K:{kp_htf:.1f}→{k_htf:.1f})")

        # BTC divergence sebagai tambahan konfirmasi
        stoch_div_bull, div_type_bull, div_reason_bull = detect_stoch_divergence(
            df_htf, stoch_htf, "BULLISH")
        stoch_div_bear, div_type_bear, div_reason_bear = detect_stoch_divergence(
            df_htf, stoch_htf, "BEARISH")

        if stoch_div_bull and reversal in ("BULLISH", "NONE"):
            if reversal == "NONE":
                reversal = "BULLISH"
            rev_reasons.append(div_reason_bull)
        if stoch_div_bear and reversal in ("BEARISH", "NONE"):
            if reversal == "NONE":
                reversal = "BEARISH"
            rev_reasons.append(div_reason_bear)

        result["reversal_signal"] = reversal
        result["reversal_reason"] = " | ".join(rev_reasons) if rev_reasons else "Belum ada konfirmasi reversal"

        # ── Blok Logic ──────────────────────────────────────────────────────
        # 1. BTC di SUPPLY → blok semua LONG altcoin
        # 2. BTC di DEMAND → blok semua SHORT altcoin
        # 3. BTC RANGING + belum ada reversal signal → blok SEMUA (ambiguous)
        # 4. Stoch OVERBOUGHT di HTF tanpa konfirmasi → blok LONG
        # 5. Stoch OVERSOLD di HTF tanpa konfirmasi → blok SHORT

        block_long  = False
        block_short = False
        block_reason_parts = []
        ambiguous = False

        if in_supply:
            block_long = True
            block_reason_parts.append(f"BTC di Supply Zone HTF (supply OB/FVG) 🔴")

        if in_demand:
            block_short = True
            block_reason_parts.append(f"BTC di Demand Zone HTF (demand OB/FVG) 🟢")

        # Stoch overbought HTF → blok LONG baru
        if k_htf > STOCH_OVERBOUGHT and not (reversal == "BEARISH"):
            block_long = True
            block_reason_parts.append(f"BTC Stoch HTF OVERBOUGHT K:{k_htf:.1f} (belum bearish reversal)")

        # Stoch oversold HTF → blok SHORT baru
        if k_htf < STOCH_OVERSOLD and not (reversal == "BULLISH"):
            block_short = True
            block_reason_parts.append(f"BTC Stoch HTF OVERSOLD K:{k_htf:.1f} (belum bullish reversal)")

        # BTC ambiguous: RANGING dan belum ada reversal signal yang jelas
        if htf_bias == "RANGING" and reversal == "NONE":
            ambiguous  = True
            block_long  = True
            block_short = True
            block_reason_parts.append("BTC HTF RANGING + belum ada reversal signal ❓")

        result["block_long"]   = block_long
        result["block_short"]  = block_short
        result["block_reason"] = " | ".join(block_reason_parts) if block_reason_parts else ""
        result["ambiguous"]    = ambiguous

        # ── Setup Label ──────────────────────────────────────────────────────
        zone_str = ("📦Supply" if in_supply else "") + ("💧Demand" if in_demand else "")
        stoch_str = f"K:{k_htf:.0f}/{k_ltf:.0f}"
        rev_str = f"Rev:{reversal}" if reversal != "NONE" else "Rev:❓"
        result["setup_label"] = (
            f"BTC:{htf_bias[:4]} {zone_str or 'NoZone'} "
            f"Stoch({stoch_str}) {rev_str}"
        )

    except Exception as e:
        print(f"  ⚠️  BTC Setup fetch error: {e}")
        result["block_reason"] = f"BTC setup error: {e}"

    _btc_setup_cache = {"data": result, "ts": now}
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 16 — CORRELATION FILTER
# ═══════════════════════════════════════════════════════════════════════════

def _get_correlation_group(pair: str) -> Optional[set]:
    for group in CORRELATION_GROUPS:
        if pair in group:
            return group
    return None


def apply_correlation_filter(candidates: list) -> list:
    surviving  = []
    used_groups: dict = {}

    for cand in candidates:
        group = _get_correlation_group(cand["pair"])
        if group is None:
            surviving.append(cand)
            continue
        key      = (frozenset(group), cand["direction"])
        existing = used_groups.get(key)
        if existing is None:
            used_groups[key] = cand
        elif (cand["score"] > existing["score"] or
              (cand["score"] == existing["score"] and cand["rr"] > existing["rr"])):
            used_groups[key] = cand

    surviving.extend(used_groups.values())
    return surviving


# ██  SECTION 17 — DYNAMIC RISK & LOT SIZE
# ═══════════════════════════════════════════════════════════════════════════

_exchange_info_cache: dict = {}


def _parse_symbol_filters(s: dict) -> dict:
    """
    Parse semua filter dari satu entry symbol di exchangeInfo.

    Kunci penting:
    - lot_maxQty  : maxQty dari LOT_SIZE  (untuk LIMIT order)
    - maxQty      : maxQty untuk MARKET order = min(LOT_SIZE.maxQty, MARKET_LOT_SIZE.maxQty)
                    KECUALI jika MARKET_LOT_SIZE.maxQty × tickPrice < minNotional
                    (kontradiksi) → fallback ke LOT_SIZE.maxQty
    - minNotional : dari MIN_NOTIONAL filter, field "notional"
    - tickSize    : diambil dari pricePrecision (level symbol) jika PRICE_FILTER terlalu kasar
    """
    filters = {}

    # ── Ambil pricePrecision dari level symbol (lebih akurat untuk harga kecil) ──
    # Binance menyimpan pricePrecision di root symbol dict, bukan di filters.
    # Contoh: ADAUSDT pricePrecision=4 → tickSize efektif = 0.0001
    price_precision = s.get("pricePrecision")   # int, misal 4
    qty_precision   = s.get("quantityPrecision") # int, misal 0

    for f in s.get("filters", []):
        ft = f["filterType"]
        if ft == "LOT_SIZE":
            filters["stepSize"]    = float(f["stepSize"])
            filters["minQty"]      = float(f["minQty"])
            filters["lot_maxQty"]  = float(f["maxQty"])   # simpan LOT_SIZE.maxQty terpisah
            filters["maxQty"]      = float(f["maxQty"])   # default, akan di-update
        elif ft == "MARKET_LOT_SIZE":
            filters["mkt_maxQty"]   = float(f.get("maxQty",  9999999.0))
            filters["mkt_minQty"]   = float(f.get("minQty",  0.0))
            filters["mkt_stepSize"] = float(f.get("stepSize", 0.0))
        elif ft == "PRICE_FILTER":
            raw_tick = float(f["tickSize"])
            filters["tickSize"] = raw_tick
            filters["price_filter_tick"] = raw_tick  # simpan asli untuk referensi
            _min_p = float(f.get("minPrice", 0))
            if _min_p > 0:
                filters["minPrice"] = _min_p
        elif ft == "MIN_NOTIONAL":
            filters["minNotional"] = float(f.get("notional", 5.0))
        elif ft == "PERCENT_PRICE":
            # -4016 = PRICE_HIGHER_THAN_MULTIPLIER_UP  → price > mark * multiplierUp
            # -4024 = PRICE_LOWER_THAN_MULTIPLIER_DOWN → price < mark * multiplierDown
            # Simpan multiplier agar bisa clamp entry sebelum kirim order
            filters["multiplierUp"]   = float(f.get("multiplierUp",   1.05))
            filters["multiplierDown"] = float(f.get("multiplierDown", 0.95))

    # ── Koreksi tickSize menggunakan pricePrecision jika PRICE_FILTER terlalu kasar ──
    # Kasus valid: ADAUSDT PRICE_FILTER tickSize=0.1, tapi pricePrecision=4
    # → harga 0.2443 dibulatkan ke 0.2 (salah) → gunakan 10^-pricePrecision = 0.0001
    #
    # BUG FIX (-4014 BTCUSDT): BTCUSDT pricePrecision=2 → precise_tick=0.01,
    # padahal PRICE_FILTER tickSize=0.10 (yang benar). Override ke 0.01 menyebabkan
    # harga seperti 107890.33 dikirim, padahal Binance require kelipatan 0.10 → -4014.
    #
    # Solusi: override hanya jika raw_tick dari PRICE_FILTER > precise_tick DAN
    # harga pair < $10. Pair mahal (BTC/ETH) harus selalu pakai PRICE_FILTER tick.
    # PRICE_FILTER adalah source of truth — precise_tick hanya fallback untuk
    # altcoin murah di mana PRICE_FILTER bisa terlalu kasar.
    raw_tick = filters.get("tickSize", 0.0001)
    if price_precision is not None:
        precise_tick = 10 ** (-int(price_precision))
        if precise_tick < raw_tick:
            # Override hanya jika raw_tick dari PRICE_FILTER tampak tidak masuk akal
            # untuk pair ini — yaitu jika pair harga rendah (< $10) dan precise_tick
            # lebih halus. Untuk pair mahal (BTC/ETH), PRICE_FILTER sudah benar.
            # Ambil harga terbaik yang tersedia untuk keputusan ini:
            _ref_price = filters.get("minPrice", 0.0)
            # Jika minPrice tidak ada, estimasi dari tick magnitude:
            # raw_tick=0.1 → pair mungkin altcoin; raw_tick=0.1 + precise_tick=0.0001 → ADA
            # raw_tick=0.1 + precise_tick=0.01 → mungkin pair mahal (BTC pricePrecision=2)
            # Heuristic: jika precise_tick >= 0.01 (pricePrecision <= 2), pair kemungkinan
            # mahal dan PRICE_FILTER-nya valid — jangan override.
            _likely_expensive = (precise_tick >= 0.01)
            if not _likely_expensive:
                # pricePrecision lebih granular dari PRICE_FILTER → pakai pricePrecision
                filters["tickSize"] = precise_tick

    # ── Simpan quantityPrecision — dipakai untuk format_qty agar tidak -1111 ──
    # Binance Futures memvalidasi quantity berdasarkan quantityPrecision (root symbol),
    # bukan hanya stepSize. Contoh: RENDERUSDT quantityPrecision=0 → qty harus bulat.
    # Jika quantityPrecision tidak ada di response (misal testnet), derive dari stepSize.
    if qty_precision is not None:
        filters["quantityPrecision"] = int(qty_precision)
    elif "stepSize" in filters:
        # Derive dari stepSize: 0.001 → prec=3, 1.0 → prec=0, 0.01 → prec=2
        import math as _m
        _s = filters["stepSize"]
        _derived = max(0, round(-_m.log10(_s))) if _s > 0 else 3
        filters["quantityPrecision"] = _derived

    mkt_max  = filters.get("mkt_maxQty",   9999999.0)   # tetap disimpan di dict untuk cek di execute_trade
    mkt_min  = filters.pop("mkt_minQty",   0.0)
    mkt_step = filters.pop("mkt_stepSize", 0.0)

    lot_max      = filters.get("lot_maxQty", 9999999.0)
    min_notional = filters.get("minNotional", 5.0)
    tick_size    = filters.get("tickSize", 0.0001)

    # Simpan mkt_stepSize terpisah — dipakai untuk format quantity MARKET order.
    # MARKET_LOT_SIZE.stepSize bisa lebih besar dari LOT_SIZE.stepSize (misal 1 vs 0.001).
    # Binance memvalidasi quantity MARKET order berdasarkan MARKET_LOT_SIZE.stepSize.
    # Jika mkt_step = 0 atau tidak ada → jangan simpan, biarkan execute_trade fallback ke stepSize
    if mkt_step > 0:
        filters["mkt_stepSize"] = mkt_step
    else:
        # Pastikan tidak ada key mkt_stepSize dengan nilai 0 di cache
        filters.pop("mkt_stepSize", None)

    # stepSize (untuk LIMIT order & lot sizing): tetap pakai LOT_SIZE.stepSize
    # Jangan override dengan mkt_step karena itu khusus MARKET order validation

    # minQty: pakai terbesar antara LOT_SIZE dan MARKET_LOT_SIZE
    if mkt_min > 0:
        filters["minQty"] = max(filters.get("minQty", 0.0), mkt_min)

    # maxQty untuk MARKET order:
    # Jika MARKET_LOT_SIZE.maxQty sangat kecil sehingga notionalnya < minNotional,
    # itu berarti MARKET_LOT_SIZE.maxQty bukan hard limit untuk akun normal
    # (biasanya limit untuk HFT/algo). Gunakan LOT_SIZE.maxQty.
    if mkt_max < lot_max:
        # Estimasi notional maksimal dengan MARKET_LOT_SIZE.maxQty
        # pakai tickSize sebagai proxy harga minimum (harga nyata akan lebih besar)
        # Jika mkt_max * tickSize < minNotional → kontradiksi → pakai lot_maxQty
        if tick_size > 0 and (mkt_max * tick_size) < min_notional:
            # Kontradiksi: MARKET_LOT_SIZE.maxQty terlalu kecil, fallback ke LOT_SIZE.maxQty
            filters["maxQty"] = lot_max
        else:
            filters["maxQty"] = mkt_max
    else:
        filters["maxQty"] = lot_max

    return filters


def load_exchange_info():
    global _exchange_info_cache
    try:
        info = api_get("/fapi/v1/exchangeInfo")
        for s in info.get("symbols", []):
            filters = _parse_symbol_filters(s)
            _exchange_info_cache[s["symbol"]] = filters
            if s["symbol"] in ("APTUSDT", "XLMUSDT", "PEPEUSDT", "SHIBUSDT", "GALAUSDT", "COMPUSDT", "ADAUSDT", "ETCUSDT"):
                print(f"  📋 {s['symbol']} filters: {filters}")
        print(f"✅ Exchange info loaded: {len(_exchange_info_cache)} symbols")
    except Exception as e:
        print(f"⚠️ Gagal load exchange info: {e}")

def get_lot_filters(symbol: str) -> dict:
    if not _exchange_info_cache:
        load_exchange_info()
    return _exchange_info_cache.get(symbol, {
        "stepSize":    0.001,
        "minQty":      0.001,
        "maxQty":      9999999.0,
        "minNotional": 5.0,
        "tickSize":    0.01,
    })


# Set berisi symbol yang sudah terbukti error 400 berkali-kali (cooldown auto-skip)
_untradeable_symbols: dict = {}   # symbol -> fail_count

# ── SL Blacklist: pair yang kena SL beruntun → cooldown ekstra ───────────────
# Struktur: { "SOLUSDT": {"count": 2, "first_sl_ts": 1700000000.0} }
# Jika 1 pair kena SL 2× dalam 48 jam → cooldown 24 jam extra (blok semua arah)
_pair_sl_tracker: dict = {}
SL_BLACKLIST_WINDOW_HOURS = 48    # window deteksi SL beruntun (jam)
SL_BLACKLIST_TRIGGER      = 2     # jumlah SL dalam window yang trigger blacklist
SL_BLACKLIST_COOLDOWN_HOURS = 24  # lama cooldown ekstra setelah kena blacklist


def record_pair_sl(symbol: str):
    """1:1 DENGAN BASE: blacklist SL beruntun (cooldown ekstra 24j, blok semua
    arah) DIHAPUS — base tidak punya mekanisme ini. Dibiarkan sebagai no-op
    supaya pemanggil lama tidak error."""
    return


def validate_pair_tradeable(symbol: str, price: float) -> bool:
    """
    Skip pair yang sudah gagal ≥3 kali berturut-turut.
    Reset setiap 6 jam. Bukan skip permanen — Binance bisa update filter kapan saja.
    """
    import time as _time
    now = _time.time()
    entry = _untradeable_symbols.get(symbol)
    if entry:
        count, first_fail = entry
        # Reset setelah 6 jam
        if now - first_fail > 21600:
            del _untradeable_symbols[symbol]
            return True
        if count >= 3:
            return False
    return True


_global_order_fail_streak: int = 0
_global_fail_alert_active: bool = False
GLOBAL_FAIL_STREAK_ALERT_THRESHOLD = 5   # kirim alert kalau order gagal berturut-turut (across semua pair)


def _note_order_fail_streak():
    """
    Hitung kegagalan order BERTURUT-TURUT lintas semua pair (bukan per-pair).
    Kalau banyak gagal beruntun → kemungkinan besar masalah struktural
    (API key salah/expired, IP ke-block, saldo habis, dll) — bukan cuma
    satu pair yang kebetulan gagal. User perlu tahu SEGERA, bukan nunggu
    ketemu manual pas cek /status.
    """
    global _global_order_fail_streak, _global_fail_alert_active
    _global_order_fail_streak += 1
    if _global_order_fail_streak >= GLOBAL_FAIL_STREAK_ALERT_THRESHOLD and not _global_fail_alert_active:
        _global_fail_alert_active = True
        send_telegram_raw(
            f"🚨 <b>PERINGATAN — Order Gagal Berturut-turut</b>\n"
            f"{'─'*34}\n"
            f"❌ <b>{_global_order_fail_streak}x</b> order gagal beruntun (lintas pair).\n"
            f"Kemungkinan penyebab:\n"
            f"  • API key salah / sudah expired / permission Futures belum aktif\n"
            f"  • IP server belum/tidak lagi di-whitelist di Binance\n"
            f"  • Saldo margin habis atau tidak cukup\n"
            f"  • Binance sedang bermasalah (maintenance/downtime)\n"
            f"{'─'*34}\n"
            f"🔎 Cek /status untuk detail, atau cek log Railway untuk pesan error lengkap.\n"
            f"Alert ini hanya dikirim sekali — akan reset otomatis begitu ada order sukses."
        )
        print(f"🚨 GLOBAL FAIL STREAK ALERT — {_global_order_fail_streak}x gagal beruntun")


def _note_order_success_streak():
    """Reset streak kegagalan global begitu ada 1 order yang sukses dieksekusi."""
    global _global_order_fail_streak, _global_fail_alert_active
    if _global_order_fail_streak > 0 or _global_fail_alert_active:
        _global_order_fail_streak = 0
        _global_fail_alert_active = False


def record_pair_fail(symbol: str):
    """Catat satu kegagalan order untuk symbol ini."""
    import time as _time
    now = _time.time()
    entry = _untradeable_symbols.get(symbol)
    if entry:
        count, first_fail = entry
        _untradeable_symbols[symbol] = (count + 1, first_fail)
    else:
        _untradeable_symbols[symbol] = (1, now)
    count = _untradeable_symbols[symbol][0]
    if count >= 3:
        print(f"  ⚠️  [{symbol}] di-skip sementara setelah {count}x gagal (reset 6 jam)")
    _note_order_fail_streak()


def _step_precision(step_size: float) -> int:
    """Hitung jumlah desimal dari step_size (misal 0.001 → 3, 1.0 → 0)."""
    if step_size <= 0:
        return 8
    return max(0, round(-math.log10(step_size)))


def round_lot_to_step(quantity: float, step_size: float) -> float:
    """Floor ke step_size terdekat, bebas floating-point drift."""
    if step_size <= 0:
        return quantity
    precision = _step_precision(step_size)
    # Gunakan Decimal untuk menghindari floating-point error
    from decimal import Decimal, ROUND_DOWN
    d_qty  = Decimal(str(quantity))
    d_step = Decimal(str(step_size))
    qty = float((d_qty / d_step).to_integral_value(rounding=ROUND_DOWN) * d_step)
    return round(qty, precision)


def round_lot_up_to_step(quantity: float, step_size: float) -> float:
    """Ceil ke step_size terdekat, bebas floating-point drift."""
    if step_size <= 0:
        return quantity
    precision = _step_precision(step_size)
    from decimal import Decimal, ROUND_UP
    d_qty  = Decimal(str(quantity))
    d_step = Decimal(str(step_size))
    qty = float((d_qty / d_step).to_integral_value(rounding=ROUND_UP) * d_step)
    return round(qty, precision)


def format_qty(quantity: float, step_size: float, qty_precision: int = None) -> str:
    """
    Format quantity sebagai string dengan presisi exact sesuai stepSize + quantityPrecision.

    Aturan Binance:
    - stepSize menentukan granularitas lot (misal 0.001 = 3 desimal)
    - quantityPrecision (root symbol) adalah hard cap desimal untuk MARKET order
    - Jika keduanya ada, ambil yang PALING KETAT (desimal terkecil = presisi lebih sedikit)
    - Floor quantity sebelum format — jangan hanya round, Binance tidak suka rounding naik

    Contoh bug yang dicegah:
    - HYPEUSDT stepSize=0.0001 tapi quantityPrecision=1 → qty "100.7848" → REJECT -1111
      Fix: min(4, 1) = 1 → floor → "100.7" ✅
    - RENDERUSDT stepSize=0.001 tapi quantityPrecision=0 → qty "50.113" → REJECT -1111
      Fix: min(3, 0) = 0 → floor → "50" ✅
    """
    precision = _step_precision(step_size)
    if qty_precision is not None:
        precision = min(precision, int(qty_precision))
    # FLOOR dulu sebelum format — Binance strict, tidak boleh ada digit lebih
    import math as _mf
    if precision >= 0:
        factor = 10 ** precision
        quantity = _mf.floor(quantity * factor) / factor
    return f"{quantity:.{precision}f}"


def format_price(price: float, tick_size: float) -> str:
    """
    Format harga sebagai string dengan presisi exact sesuai tickSize.

    KRITIS: Harga HARUS merupakan kelipatan exact tickSize — Binance menolak
    harga yang tidak snap ke tickSize dengan error -4014 "Price not increased
    by tick size". Gunakan Decimal untuk snap yang bebas floating-point error.

    Contoh: BTCUSDT tickSize=0.10 → harga 83241.33 → snap ke 83241.3 → "83241.3"
    Contoh: IMXUSDT tickSize=0.0001 → harga 0.13050000001 → "0.1305"
    """
    if tick_size <= 0:
        # fallback: hitung presisi dari nilai harga itu sendiri
        if price >= 100:    precision = 2
        elif price >= 10:   precision = 3
        elif price >= 1:    precision = 4
        elif price >= 0.1:  precision = 4
        elif price >= 0.01: precision = 5
        else:               precision = 8
        return f"{price:.{precision}f}"
    # Snap ke kelipatan tickSize menggunakan Decimal (bebas float drift)
    from decimal import Decimal, ROUND_HALF_UP
    precision = max(0, round(-math.log10(tick_size)))
    precision = min(precision, 8)
    d_price = Decimal(str(price))
    d_tick  = Decimal(str(tick_size))
    snapped = (d_price / d_tick).to_integral_value(rounding=ROUND_HALF_UP) * d_tick
    return f"{float(round(snapped, precision)):.{precision}f}"


def round_price_to_tick(price: float, tick_size: float) -> float:
    """
    Snap harga ke kelipatan tickSize terdekat menggunakan Decimal untuk
    menghindari floating-point error.

    Binance error -4014 "Price not increased by tick size" terjadi ketika
    harga limit order bukan kelipatan exact dari tickSize.
    Contoh: tickSize=0.10, harga 83241.33 → harus 83241.3 (bukan round biasa
    yang bisa menghasilkan 83241.30000000001 akibat float drift).
    """
    if tick_size <= 0:
        return price
    from decimal import Decimal, ROUND_HALF_UP
    precision = max(0, round(-math.log10(tick_size)))
    precision = min(precision, 8)
    d_price = Decimal(str(price))
    d_tick  = Decimal(str(tick_size))
    snapped = (d_price / d_tick).to_integral_value(rounding=ROUND_HALF_UP) * d_tick
    return float(round(snapped, precision))


def floor_price_to_tick(price: float, tick_size: float) -> float:
    """
    Snap harga ke kelipatan tickSize TERDEKAT DI BAWAH (floor), bukan nearest.

    WAJIB dipakai setiap kali clamp ke batas ATAS (mis. PERCENT_PRICE
    multiplierUp, atau -4016 "limit price can't be higher than X").
    round_price_to_tick (ROUND_HALF_UP) BISA membulatkan ke ATAS batas asli
    Binance — contoh nyata: WIFUSDT mark=0.173, maxAllowed=0.1816706,
    tickSize=0.1 → round_price_to_tick membulatkan ke 0.2 (> maxAllowed asli)
    sehingga order tetap ditolak -4016 walau sudah "di-clamp". floor selalu
    menghasilkan harga <= batas asli.
    """
    if tick_size <= 0:
        return price
    from decimal import Decimal, ROUND_DOWN
    precision = max(0, round(-math.log10(tick_size)))
    precision = min(precision, 8)
    d_price = Decimal(str(price))
    d_tick  = Decimal(str(tick_size))
    snapped = (d_price / d_tick).to_integral_value(rounding=ROUND_DOWN) * d_tick
    return float(round(snapped, precision))


def ceil_price_to_tick(price: float, tick_size: float) -> float:
    """
    Snap harga ke kelipatan tickSize TERDEKAT DI ATAS (ceil), bukan nearest.

    WAJIB dipakai setiap kali clamp ke batas BAWAH (mis. PERCENT_PRICE
    multiplierDown, atau -4024 "limit price can't be lower than X"), supaya
    hasil clamp tidak jatuh di bawah batas asli Binance akibat pembulatan.
    """
    if tick_size <= 0:
        return price
    from decimal import Decimal, ROUND_UP
    precision = max(0, round(-math.log10(tick_size)))
    precision = min(precision, 8)
    d_price = Decimal(str(price))
    d_tick  = Decimal(str(tick_size))
    snapped = (d_price / d_tick).to_integral_value(rounding=ROUND_UP) * d_tick
    return float(round(snapped, precision))


def get_streak_risk_multiplier() -> float:
    """
    Faktor pengali risk per trade berdasarkan win/lose streak berjalan.

    SEBELUMNYA: logika ini (lose streak → risk dipotong 50%, win streak →
    risk naik 50%) ada di dalam get_dynamic_risk(), tapi get_dynamic_risk()
    cuma dipanggil dari calculate_lot() — dan calculate_lot() TIDAK PERNAH
    dipanggil di jalur eksekusi live manapun (execute_trade() punya
    perhitungan lot sendiri berbasis MAX_SL_LOSS_PCT yang tidak baca streak
    sama sekali). Akibatnya proteksi "kecilkan size pas lagi kalah beruntun"
    ini sebenarnya TIDAK PERNAH AKTIF di trading real — cuma kelihatan aktif
    di kode.

    Fix: fungsi ini dipanggil langsung dari execute_trade() sizing (lihat
    'GUARD FINAL' & max_loss_usdt di bawah), supaya proteksi ini beneran
    jalan di posisi yang sungguhan dibuka.

    - lose_streak ≥ 3 → 0.5x  (prioritas: proteksi modal dulu)
    - win_streak  ≥ 3 → 1.5x  (risk naik moderat saat sedang on-fire)
    - selain itu      → 1.0x  (normal)
    """
    if bot_state.get("lose_streak", 0) >= 3:
        return 0.5
    if bot_state.get("win_streak", 0) >= 3:
        return 1.5
    return 1.0


def get_dynamic_risk():
    """
    Hitung risk per trade berdasarkan jumlah posisi yang BOLEH dibuka (dynamic_max).

    Logika:
      RISK_PER_TRADE = total risk maksimal jika semua posisi kena SL sekaligus.
      Per trade = RISK_PER_TRADE / dynamic_max_trades

      Pakai get_dynamic_max_trades() bukan MAX_OPEN_TRADES langsung,
      agar jika user set /maxopentrade 3 dengan balance $50,
      risk per trade = 30%/3 = 10% → notional $50×10%×15x = $75 (terlalu kecil!)

      FIX: gunakan max(dynamic_max, 1) tapi cap ke actual open + 1
      Jika saat ini hanya 0 posisi terbuka dan mau buka 1,
      risk per trade = RISK_PER_TRADE / 1 = 30% → notional $225 ✅
    """
    dynamic_max = get_dynamic_max_trades()
    current_open = count_open_positions()
    # Slots yang masih tersedia — ini menentukan berapa "slot" yang bersaing
    # Jika sudah ada 2 posisi dan max=3 → slot tersisa 1 → risk untuk 1 trade
    slots_remaining = max(1, dynamic_max - current_open)
    # Risk per trade = total risk dibagi slot yang masih tersedia
    base = RISK_PER_TRADE / slots_remaining
    # Tetap ada batas atas: tidak boleh lebih dari RISK_PER_TRADE penuh (jika slots=1)
    base = min(base, RISK_PER_TRADE)
    if bot_state["lose_streak"] >= 3:
        return base * 0.5
    if bot_state["win_streak"] >= 3:
        return base * 1.5
    return base


def calculate_lot(entry, sl, balance, symbol=None, leverage=None):
    """
    Hitung lot size dengan money management adaptif.

    Sistem:
    1. Leverage dipilih otomatis berdasarkan harga (get_leverage_for_price)
    2. risk_usdt = totalBalance x (RISK_PER_TRADE / MAX_OPEN_TRADES)
       -> risk per trade sudah dibagi jumlah max posisi agar total exposure
          tidak melebihi RISK_PER_TRADE meski semua posisi kena SL sekaligus
    3. Lot = risk_usdt / SL_distance -> berapa unit supaya loss = risk_usdt
    4. Notional di-cap: balance x leverage x MAX_NOTIONAL_RATIO (di execute_trade)
    """
    if entry <= 0:
        return 0

    if leverage is None:
        leverage = get_leverage_for_price(entry)

    risk_usdt = balance * get_dynamic_risk()

    # ── PROTEKSI 1: enforce minimum SL distance ───────────────────────────────
    raw_distance = abs(entry - sl)
    min_distance = entry * MIN_SL_DISTANCE_PCT
    effective_distance = max(raw_distance, min_distance)

    # Lot dari risk: loss jika SL kena = lot × distance = risk_usdt
    raw_lot = risk_usdt / effective_distance

    # ── PROTEKSI 2: cap notional terhadap leverage yang dipilih ──────────────
    # Gunakan full leverage capacity (MAX_NOTIONAL_RATIO diaplikasikan di execute_trade)
    max_notional = balance * leverage
    raw_lot      = min(raw_lot, max_notional / entry)

    # ── PROTEKSI 3: maxQty dari exchange info ─────────────────────────────────
    # Gunakan lot_maxQty (LOT_SIZE filter), BUKAN maxQty (MARKET_LOT_SIZE).
    # MARKET_LOT_SIZE.maxQty untuk pair nano-price (PEPE/SHIB) sangat kecil
    # sehingga raw_lot ter-cap kecil sebelum sampai execute_trade.
    # lot_maxQty adalah true hard cap per order untuk akun normal.
    if symbol:
        filters   = get_lot_filters(symbol)
        lot_max   = filters.get("lot_maxQty", filters.get("maxQty", 9999999.0))
        step_size = filters.get("stepSize", 0.001)
        raw_lot   = min(raw_lot, lot_max)
        raw_lot   = round_lot_to_step(raw_lot, step_size)

    return max(raw_lot, 0)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 18 — EXECUTE TRADE
# ═══════════════════════════════════════════════════════════════════════════

def set_leverage(symbol, leverage):
    """
    Set leverage dan verifikasi berhasil.
    Return: leverage yang berhasil di-set (bisa lebih rendah dari request jika bracket limit).
    """
    try:
        resp = api_post("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        actual_lev = int(resp.get("leverage", leverage))
        if actual_lev != leverage:
            print(f"  ⚠️  [{symbol}] Leverage di-set {leverage}x tapi Binance return {actual_lev}x (bracket limit)")
        return actual_lev
    except Exception as e:
        err_str = str(e)
        print(f"⚠️ Set leverage error [{symbol}]: {e}")
        # -4028 = leverage sudah sama, tidak perlu di-set ulang → OK
        if "-4028" in err_str:
            return leverage
        return None


def _cancel_algo_orders(symbol: str):
    """
    Cancel semua algo/conditional orders untuk symbol.

    Layer 1: Bulk cancel via DELETE /fapi/v1/algoOpenOrders
    Layer 2: Fetch list → cancel satu per satu via algoId
    404 = testnet atau akun tidak support → diabaikan diam-diam (fallback ke allOpenOrders)
    """
    # ── Layer 1: Bulk cancel ──────────────────────────────────────────────────
    try:
        api_delete("/fapi/v1/algoOpenOrders", {"symbol": symbol})
        return  # berhasil, selesai
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        if code == 404:
            pass  # testnet / tidak support — lanjut ke layer 2
        else:
            print(f"  ⚠️ Bulk cancel algo {symbol} HTTP {code} — coba per algoId")
    except Exception:
        pass

    # ── Layer 2: Fetch list → cancel per algoId ───────────────────────────────
    try:
        algo_resp = api_get("/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True)
        if isinstance(algo_resp, list):
            orders = algo_resp
        elif isinstance(algo_resp, dict):
            orders = algo_resp.get("orders", [])
        else:
            orders = []

        for o in orders:
            algo_id = o.get("algoId") or o.get("orderId")
            if not algo_id:
                continue
            try:
                api_delete("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})
            except Exception as ce:
                print(f"  ⚠️  [{symbol}] Gagal cancel algoId {algo_id}: {ce}")
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            pass  # testnet — aman diabaikan
        else:
            print(f"  ⚠️ Fetch algo orders {symbol}: {e}")
    except Exception as e:
        print(f"  ⚠️ Fetch algo orders {symbol}: {e}")


def cancel_sl_orders_only(symbol: str):
    """
    Cancel HANYA order bertipe SL (STOP_MARKET/STOP), biarkan TP1/TP2 tetap
    hidup di Binance.

    FIX: sebelumnya proses "geser SL ke breakeven" saat TP1 hit memanggil
    cancel_open_orders(symbol) — itu membatalkan SEMUA order simbol tsb
    (SL DAN TP2 sekaligus), padahal levelnya TP2 sama sekali tidak berubah,
    cuma SL yang perlu diganti. Ini bikin ada jeda ("closing window") antara
    TP2 lama dibatalkan sampai TP2 baru dipasang ulang — kalau di jeda itu
    ada gangguan (network lambat, API server macet), TP2 bisa gagal
    dipasang ulang sementara TP2 lamanya udah kadung hilang, sehingga posisi
    sempat jalan tanpa proteksi TP2 walau posisinya masih hidup.

    Fungsi ini hanya menyasar order SL — order TP1/TP2 tidak pernah disentuh,
    jadi tidak ada jeda "TP2 sempat hilang" sama sekali untuk kasus ganti SL.
    """
    # ── Algo orders (SL/TP via algoOrder endpoint) — filter tipe SL saja ──────
    try:
        algo_resp = api_get("/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True)
        if isinstance(algo_resp, list):
            _orders = algo_resp
        elif isinstance(algo_resp, dict):
            _orders = algo_resp.get("orders", [])
        else:
            _orders = []
        for o in _orders:
            _o_type = str(o.get("type") or o.get("orderType") or "").upper()
            if "STOP" not in _o_type:
                continue  # bukan SL — biarkan (TP1/TP2 tidak disentuh)
            algo_id = o.get("algoId") or o.get("orderId")
            if not algo_id:
                continue
            try:
                api_delete("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})
            except Exception as ce:
                print(f"  ⚠️  [{symbol}] Gagal cancel SL algoId {algo_id}: {ce}")
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            pass  # testnet — lanjut ke standard order di bawah
        else:
            print(f"  ⚠️ Fetch algo orders (SL-only cancel) {symbol}: {e}")
    except Exception as e:
        print(f"  ⚠️ Fetch algo orders (SL-only cancel) {symbol}: {e}")

    # ── Standard orders (fallback non-algo) — filter tipe SL saja ─────────────
    try:
        open_ords = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
        for o in open_ords:
            _o_type = str(o.get("type") or "").upper()
            if "STOP" not in _o_type:
                continue  # bukan SL — biarkan (TP1/TP2 tidak disentuh)
            oid = o.get("orderId")
            if oid:
                try:
                    api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": oid})
                    time.sleep(0.05)
                except Exception as ce:
                    print(f"  ⚠️ Cancel SL orderId {oid} {symbol}: {ce}")
    except Exception as fe:
        print(f"  ⚠️ Fetch openOrders (SL-only cancel) {symbol}: {fe}")


def cancel_open_orders(symbol: str):
    # ── Step 1: cancel algo orders (SL/TP via algoOrder endpoint) ────────────
    _cancel_algo_orders(symbol)
    time.sleep(0.1)

    # ── Step 2: cancel per orderId (STOP_MARKET, TAKE_PROFIT_MARKET, dll) ────
    # Tidak andalkan bulk allOpenOrders — conditional orders (reduceOnly=true)
    # sering tidak ikut bulk cancel. Per orderId adalah cara paling pasti.
    try:
        open_ords = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
        for o in open_ords:
            oid = o.get("orderId")
            if oid:
                try:
                    api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": oid})
                    time.sleep(0.05)
                except Exception as ce:
                    print(f"⚠️ Cancel orderId {oid} {symbol}: {ce}")
    except Exception as fe:
        print(f"⚠️ Fetch openOrders {symbol}: {fe}")

    # ── Step 3: bulk sebagai catch-all tambahan ───────────────────────────────
    try:
        api_delete("/fapi/v1/allOpenOrders", {"symbol": symbol})
    except Exception:
        pass


def _classify_conditional_order(o: dict) -> str | None:
    """Klasifikasi order jadi 'SL' atau 'TP' berdasarkan field type/orderType."""
    t = str(o.get("type") or o.get("orderType") or "").upper()
    if "STOP" in t:
        return "SL"
    if "TAKE_PROFIT" in t:
        return "TP"
    return None


def cleanup_duplicate_position_orders(dry_run: bool = False, silent_if_clean: bool = False) -> dict:
    """
    /cleanupduplicate — bersihkan SL/TP CONDITIONAL ORDER yang MENUMPUK pada
    simbol yang MEMANG punya posisi aktif.

    Beda dengan /cleanuporders (cleanup_stale_orders): itu cuma urus symbol
    yang SAMA SEKALI TIDAK punya posisi (orphan). Symbol yang punya posisi
    aktif tidak pernah disentuh — padahal di situlah SL/TP suka menumpuk,
    biasanya gara-gara retry loop pasang SL/TP (lihat _activate_position_from_limit)
    yang timeout di sisi client padahal order sudah sukses di Binance, lalu
    retry ulang tanpa cancel dulu → order lama & baru numpuk bareng.

    STRATEGI AMAN:
    - Untuk tiap symbol dengan posisi aktif, kumpulkan semua order SL-type
      dan TP-type (regular + algoOrder).
    - SL harusnya cuma 1 → kalau lebih, SISAKAN 1 yang paling cocok dengan
      active_positions[symbol]["sl"] saat ini (atau paling baru kalau tidak
      ada acuan), sisanya dibatalkan.
    - TP harusnya maks 2 (TP1+TP2) → kalau lebih, sisakan 2 yang paling
      cocok dengan tp1/tp2 tercatat, sisanya dibatalkan.
    - TIDAK PERNAH membatalkan SEMUA SL/TP sekaligus — hanya membuang
      KELEBIHAN. Kalau order-nya cuma pas (≤1 SL, ≤2 TP), symbol dilewati.
    - Catatan: untuk akun hedge-mode (positionSide LONG & SHORT terpisah di
      symbol yang sama), fungsi ini masih menghitung per-symbol gabungan —
      cukup aman karena hanya trim kelebihan, tapi kalau kamu pakai hedge
      mode dan curiga ada false-positive, cek dulu via dry_run=True.
    """
    result = {"checked": 0, "symbols_cleaned": [], "cancelled": 0, "errors": []}

    try:
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
        symbols_with_position = sorted({
            p["symbol"] for p in all_pos if abs(float(p.get("positionAmt", 0))) > 0
        })
    except Exception as e:
        print(f"  ⚠️  cleanup_duplicate_position_orders: gagal ambil posisi: {e}")
        # FIX: kalau ini auto-cleanup (silent_if_clean=True), gangguan 408 testnet
        # yang flaky bukan hal actionable buat user — cukup log ke console, jangan
        # spam Telegram. Kalau user pencet /cleanupduplicate manual, tetap dikabari.
        if not silent_if_clean:
            send_telegram_raw(f"❌ <b>/cleanupduplicate gagal</b>\nTidak bisa ambil posisi: <code>{e}</code>")
        return result

    if not symbols_with_position:
        if not silent_if_clean:
            send_telegram_raw("📭 Tidak ada posisi aktif — tidak ada yang perlu dicek.")
        return result

    report_lines = []

    for symbol in symbols_with_position:
        result["checked"] += 1
        try:
            regular = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True) or []
        except Exception as e:
            result["errors"].append(f"{symbol} (fetch regular): {e}")
            regular = []
        try:
            algo_resp = api_get("/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True)
            algo_list = algo_resp if isinstance(algo_resp, list) else \
                        (algo_resp.get("orders", []) if isinstance(algo_resp, dict) else [])
        except Exception:
            algo_list = []   # 404 di testnet = normal, bukan error

        combined = []
        for o in regular:
            cat = _classify_conditional_order(o)
            if cat is None:
                continue
            combined.append({
                "src": "regular", "cat": cat,
                "trigger": float(o.get("stopPrice", 0) or 0),
                "id": o.get("orderId"),
                "time": o.get("time") or o.get("updateTime") or 0,
            })
        for o in algo_list:
            cat = _classify_conditional_order(o)
            if cat is None:
                continue
            combined.append({
                "src": "algo", "cat": cat,
                "trigger": float(o.get("triggerPrice", o.get("stopPrice", 0)) or 0),
                "id": o.get("algoId") or o.get("orderId"),
                "time": o.get("bookTime") or o.get("time") or 0,
            })

        sl_orders = [o for o in combined if o["cat"] == "SL"]
        tp_orders = [o for o in combined if o["cat"] == "TP"]

        pos_ref = active_positions.get(symbol, {})
        exp_sl  = pos_ref.get("sl")
        exp_tp1 = pos_ref.get("tp1")
        exp_tp2 = pos_ref.get("tp2")
        # FIX RACE: order ID yang bot SENDIRI simpan sebagai TP1/TP2/SL "resmi"
        # saat ini (termasuk hasil pasang-ulang oleh self-heal trailing monitor).
        # ID ini adalah sumber kebenaran paling akurat — jauh lebih presisi
        # daripada menebak dari jarak harga trigger, yang bisa salah kalau TP1
        # baru dipasang ulang dengan harga fresh (beda tipis dari harga awal).
        ref_sl  = pos_ref.get("sl_order_ref")
        ref_tp1 = pos_ref.get("tp1_order_ref")
        ref_tp2 = pos_ref.get("tp2_order_ref")
        known_refs = {r for r in (ref_sl, ref_tp1, ref_tp2) if r}

        to_cancel = []

        if len(sl_orders) > 1:
            # Prioritas #1: kalau salah satu order cocok persis dengan
            # sl_order_ref yang bot simpan, itu WAJIB dipertahankan.
            _keep_by_id = [o for o in sl_orders if ref_sl and o["id"] == ref_sl]
            if _keep_by_id:
                _keep = _keep_by_id[0]
                to_cancel.extend([o for o in sl_orders if o["id"] != _keep["id"]])
            elif exp_sl:
                sl_orders.sort(key=lambda o: abs(o["trigger"] - exp_sl))
                to_cancel.extend(sl_orders[1:])   # sisakan 1 paling cocok
            else:
                sl_orders.sort(key=lambda o: o["time"], reverse=True)
                to_cancel.extend(sl_orders[1:])   # sisakan 1 paling terbaru

        if len(tp_orders) > 2:
            # Prioritas #1: pertahankan semua order yang ID-nya cocok dengan
            # tp1_order_ref / tp2_order_ref tersimpan — itu order yang bot
            # SENDIRI baru saja pasang (termasuk hasil self-heal), jadi tidak
            # boleh dianggap "duplikat lama" hanya karena harga trigger-nya
            # beda tipis dari nilai tp1/tp2 di active_positions.
            _keep_known = [o for o in tp_orders if o["id"] in known_refs]
            _rest       = [o for o in tp_orders if o["id"] not in known_refs]
            _slots_left = max(0, 2 - len(_keep_known))
            if _rest and _slots_left < len(_rest):
                targets = [t for t in (exp_tp1, exp_tp2) if t]
                if targets:
                    _rest.sort(key=lambda o: min(abs(o["trigger"] - t) for t in targets))
                else:
                    _rest.sort(key=lambda o: o["time"], reverse=True)
                to_cancel.extend(_rest[_slots_left:])
            elif not _rest and len(_keep_known) > 2:
                # Edge case: lebih dari 2 order ID dikenal (seharusnya jarang) —
                # sisakan tp1_order_ref + tp2_order_ref eksplisit saja.
                to_cancel.extend([o for o in _keep_known if o["id"] not in (ref_tp1, ref_tp2)])

        if not to_cancel:
            continue

        if dry_run:
            report_lines.append(
                f"  🔍 {symbol}: {len(to_cancel)} order duplikat terdeteksi "
                f"(SL={len(sl_orders)}, TP={len(tp_orders)}) — belum dibatalkan (dry-run)"
            )
            continue

        # FIX RACE: pegang lock supaya tidak overlap dengan manage_trailing()
        # yang mungkin lagi pasang-ulang TP1/TP2/SL untuk symbol yang sama.
        with _order_mgmt_lock:
            # Re-baca pos_ref & refs SETELAH dapat lock — kalau trailing thread
            # baru saja pasang ulang order (ref berubah) sesaat sebelum lock ini
            # didapat, jangan sampai order baru itu ikut ter-cancel di bawah.
            _pos_ref2   = active_positions.get(symbol, {})
            _fresh_refs = {
                _pos_ref2.get("sl_order_ref"),
                _pos_ref2.get("tp1_order_ref"),
                _pos_ref2.get("tp2_order_ref"),
            }
            cancelled_here = 0
            for o in to_cancel:
                if o["id"] in _fresh_refs:
                    # Ref berubah sejak dihitung di atas (race dengan self-heal
                    # yang baru saja jalan) — jangan cancel, skip demi aman.
                    continue
                try:
                    if o["src"] == "algo":
                        api_delete("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": o["id"]})
                    else:
                        api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": o["id"]})
                    cancelled_here += 1
                    result["cancelled"] += 1
                    time.sleep(0.05)
                except Exception as ce:
                    result["errors"].append(f"{symbol} (cancel {o['id']}): {ce}")

        if cancelled_here:
            result["symbols_cleaned"].append(symbol)
            report_lines.append(
                f"  🗑️ {symbol}: {cancelled_here} order duplikat dibuang "
                f"(SL sisa maks 1, TP sisa maks 2)"
            )

    if dry_run:
        if report_lines:
            send_telegram_raw(
                f"🔍 <b>Duplicate SL/TP Orders — DRY RUN</b>\n{'─'*34}\n"
                + "\n".join(report_lines) +
                f"\n{'─'*34}\nSimbol dicek: {result['checked']}\n"
                f"Ketik <code>/cleanupduplicate confirm</code> untuk beneran cancel."
            )
        else:
            send_telegram_raw(f"✅ Semua {result['checked']} posisi aktif sudah bersih, tidak ada order duplikat.")
        return result

    if report_lines:
        send_telegram_raw(
            f"🧹 <b>Duplicate SL/TP Orders Dibersihkan</b>\n{'─'*34}\n"
            + "\n".join(report_lines) +
            f"\n{'─'*34}\nSimbol dicek: {result['checked']} | Order dihapus: {result['cancelled']}"
        )
    elif not silent_if_clean:
        send_telegram_raw(f"✅ Semua {result['checked']} posisi aktif sudah bersih (SL maks 1, TP maks 2).")

    if result["errors"]:
        send_telegram_raw("⚠️ Beberapa gagal dicek/cancel:\n" + "\n".join(result["errors"][:10]))

    return result


def startup_cleanup_pending_orders():
    """
    Dipanggil SEKALI setiap kali bot boot (di main(), sebelum masuk loop
    PAUSED dan sebelum Telegram listener jalan). Tujuan: cegah limit order +
    conditional order dari sesi sebelumnya nyangkut tanpa terpantau.

    ROOT CAUSE bug "posisi nongol sendiri tanpa SL/TP (📱 di /pnl)":
    Bot SELALU mulai dalam kondisi PAUSED tiap restart. Selama PAUSED, main
    loop SENGAJA skip monitor_pending_limit_orders() (lihat komentar di loop
    utama). Kalau pending_limit_orders lama ikut ke-load dari state
    (load_state()), order itu bisa ke-fill diam-diam di Binance selagi bot
    PAUSED — tidak pernah lewat _activate_position_from_limit() (jadi SL/TP
    tidak pernah dipasang) dan TIDAK ikut ke-cancel oleh TP1-precheck logic
    (yang cuma jalan di dalam monitor_pending_limit_orders).
    Lalu waktu /start dikirim, sync_existing_positions() JUGA sengaja skip
    import symbol yang masih ada di pending_limit_orders (supaya tidak
    dobel-handle) — hasilnya posisi itu tidak pernah ke-adopsi ke
    active_positions sama sekali → selamanya 📱 (tidak dipantau bot).

    Fix: setiap kali bot boot, tuntaskan SEMUA pending_limit_orders sisa
    sesi sebelumnya sebelum lanjut:
      - Kalau ternyata SUDAH ada posisi real di Binance (order sempat fill
        sebelum restart) → JANGAN cancel apapun (bisa bikin posisi naked
        tanpa SL). Cukup keluarkan dari pending_limit_orders supaya
        sync_existing_positions() nanti (saat /start) meng-adopsi posisi ini
        + auto-pasang SL/TP kalau ternyata belum ada.
      - Kalau BELUM ada posisi (order masih murni pending / limit belum
        kesentuh) → cancel entry order + SEMUA conditional order-nya
        (cancel_open_orders sudah cover algo + regular), lalu buang dari
        pending_limit_orders. Bersih total, tidak ada sisa nyangkut.
    """
    if not pending_limit_orders:
        print("✅ Startup cleanup: tidak ada pending limit order tersisa dari sesi sebelumnya.")
        return

    print(f"🧹 Startup cleanup: {len(pending_limit_orders)} pending limit order dari sesi sebelumnya — {sorted(pending_limit_orders.keys())}")

    try:
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
        symbols_with_position = {p["symbol"] for p in all_pos if abs(float(p.get("positionAmt", 0))) > 0}
    except Exception as e:
        print(f"⚠️ Startup cleanup: gagal fetch positionRisk ({e}) — skip cleanup demi keamanan (biar tidak salah cancel)")
        return

    adopted   = []   # order sudah fill sebelum restart → dilepas, biar di-adopsi sync_existing_positions()
    cancelled = []   # order belum fill → cancel total (entry + conditional)
    failed    = []

    for symbol in list(pending_limit_orders.keys()):
        if symbol in symbols_with_position:
            print(f"  📥 [{symbol}] Sudah ada posisi real di Binance — lepas dari pending, biar di-adopsi sync_existing_positions() saat /start")
            adopted.append(symbol)
            del pending_limit_orders[symbol]
        else:
            print(f"  🗑  [{symbol}] Belum ada posisi — cancel entry order + conditional order")
            try:
                cancel_open_orders(symbol)  # cancel algo (SL/TP) + regular (limit entry)
                cancelled.append(symbol)
            except Exception as e:
                print(f"  ⚠️  [{symbol}] Gagal cancel saat startup cleanup: {e}")
                failed.append(symbol)
            del pending_limit_orders[symbol]

    try:
        save_state()
    except Exception as e:
        print(f"⚠️ Startup cleanup: gagal save_state setelah cleanup: {e}")

    if adopted or cancelled or failed:
        lines = []
        if cancelled:
            lines.append(f"🗑 Dibatalkan (belum fill): {', '.join(sorted(cancelled))}")
        if adopted:
            lines.append(f"📥 Sudah jadi posisi real (akan di-adopsi otomatis saat /start): {', '.join(sorted(adopted))}")
        if failed:
            lines.append(f"⚠️ Gagal cancel (cek manual di Binance): {', '.join(sorted(failed))}")
        send_telegram_raw(
            f"🧹 <b>Startup Cleanup — Pending Limit Orders</b>\n"
            f"{'─'*34}\n"
            + "\n".join(lines) + "\n"
            f"{'─'*34}\n"
            f"Sisa order dari sesi sebelumnya sudah dituntaskan — bot mulai bersih."
        )


def cancel_orders_for_symbol(symbol: str) -> dict:
    """
    Cancel SEMUA open order (regular + conditional/algo TP-SL) untuk 1 symbol,
    TANPA syarat apapun — tidak peduli symbol masih punya posisi aktif, pending
    limit order, atau tidak. Dipakai oleh /cancelorders <SYMBOL> untuk hapus
    TP/SL yang nyangkut secara manual & instan, tanpa harus nunggu posisi close
    atau nunggu auto-cleanup orphan (yang memang sengaja skip symbol yang masih
    ada posisi/pending-nya).

    Return dict: {"regular_total", "regular_ok", "algo_total", "algo_ok", "errors": [...]}
    """
    symbol = symbol.upper()
    result = {"regular_total": 0, "regular_ok": 0, "algo_total": 0, "algo_ok": 0, "errors": []}

    # ── Step 1: regular open orders (termasuk TAKE_PROFIT_MARKET/STOP_MARKET biasa) ──
    try:
        all_open = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
        result["regular_total"] = len(all_open)
        for o in all_open:
            oid = o.get("orderId")
            if not oid:
                continue
            try:
                api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": oid})
                result["regular_ok"] += 1
                time.sleep(0.05)
            except Exception as ce:
                result["errors"].append(f"orderId {oid}: {ce}")
        # Bulk sebagai catch-all tambahan (idempotent kalau sudah kosong)
        try:
            api_delete("/fapi/v1/allOpenOrders", {"symbol": symbol})
        except Exception:
            pass
    except Exception as fe:
        result["errors"].append(f"fetch openOrders gagal: {fe}")
        try:
            api_delete("/fapi/v1/allOpenOrders", {"symbol": symbol})
        except Exception as be:
            result["errors"].append(f"bulk cancel fallback gagal: {be}")

    # ── Step 2: algo/conditional orders (mainnet only, 404 di testnet) ──────────
    try:
        algo_resp = api_get("/fapi/v1/openAlgoOrders", {"symbol": symbol}, signed=True)
        algo_list = algo_resp if isinstance(algo_resp, list) else algo_resp.get("orders", [])
        algo_list = [o for o in algo_list if o.get("symbol") == symbol]
        result["algo_total"] = len(algo_list)
        if algo_list:
            try:
                api_delete("/fapi/v1/algoOpenOrders", {"symbol": symbol})
                result["algo_ok"] = len(algo_list)
            except Exception:
                for o in algo_list:
                    algo_id = o.get("algoId") or o.get("orderId")
                    if not algo_id:
                        continue
                    try:
                        api_delete("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id})
                        result["algo_ok"] += 1
                        time.sleep(0.05)
                    except Exception as fe:
                        result["errors"].append(f"algoId {algo_id}: {fe}")
    except requests.exceptions.HTTPError as e:
        if not (e.response is not None and e.response.status_code == 404):
            result["errors"].append(f"fetch algoOrders gagal: {e}")
    except Exception as e:
        result["errors"].append(f"fetch algoOrders gagal: {e}")

    return result


def cmd_cancelorders(parts):
    """
    /cancelorders <SYMBOL> — cancel manual & instan SEMUA open order (termasuk
    TP/SL conditional) untuk 1 pair, tanpa syarat posisi/pending apapun.
    Beda dari /cleanuporders: ini TIDAK cek apakah symbol orphan dulu — user
    yang tentukan langsung symbol mana yang mau dibersihkan order-nya.
    """
    if len(parts) < 2:
        send_telegram_raw(
            "⚠️ <b>Format salah.</b>\n"
            "Pakai: <code>/cancelorders SYMBOL</code>\n"
            "Contoh: <code>/cancelorders HYPEUSDT</code>"
        )
        return

    symbol = parts[1].upper()
    send_telegram_raw(f"🔄 <b>Cancel semua order {symbol}...</b>")

    try:
        r = cancel_orders_for_symbol(symbol)
    except Exception as e:
        send_telegram_raw(f"❌ <b>Cancel orders gagal</b>\nSymbol: {symbol}\nError: <code>{e}</code>")
        return

    if r["regular_total"] == 0 and r["algo_total"] == 0 and not r["errors"]:
        send_telegram_raw(f"📭 <b>{symbol}</b> tidak punya open order sama sekali.")
        return

    err_note = ""
    if r["errors"]:
        err_lines = "\n".join([f"  ⚠️ {e}" for e in r["errors"][:10]])
        err_note = f"\n\n<b>Errors:</b>\n{err_lines}"

    send_telegram_raw(
        f"🗑️ <b>Cancel Orders — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"Regular orders: {r['regular_ok']}/{r['regular_total']} cancelled\n"
        f"Algo/conditional (TP-SL): {r['algo_ok']}/{r['algo_total']} cancelled"
        f"{err_note}"
    )


def cleanup_stale_orders(dry_run: bool = False, silent_if_clean: bool = False):
    """
    Deteksi (dan opsional cancel) open orders untuk pair yang TIDAK memiliki
    posisi aktif di Binance DAN TIDAK sedang dalam pending limit order.

    dry_run=True  → hanya laporan, tidak cancel (default dari main loop — DIHAPUS)
    dry_run=False → cancel semua orphan (hanya dipanggil via /cleanuporders)

    ⚠️  Pending limit order TIDAK dianggap orphan — mereka memang belum punya
        posisi karena masih menunggu terisi di Binance.
    """
    try:
        # ── Ambil semua posisi yang benar-benar open ──────────────────────────
        all_pos = api_get("/fapi/v2/positionRisk", signed=True)
        symbols_with_position = set()
        for p in all_pos:
            if abs(float(p.get("positionAmt", 0))) > 0:
                symbols_with_position.add(p["symbol"])

        # ── Kecualikan symbol yang punya pending limit order ──────────────────
        # Limit order yang belum terisi memang tidak punya posisi — bukan orphan!
        symbols_pending_limit = set(pending_limit_orders.keys())
        symbols_safe = symbols_with_position | symbols_pending_limit

        print(f"  🔍 cleanup: posisi aktif={sorted(symbols_with_position) or 'none'} | pending limit={sorted(symbols_pending_limit) or 'none'}")

        # ── Ambil semua open regular orders ──────────────────────────────────
        stale_symbols_regular = set()
        open_orders = []
        try:
            open_orders = api_get("/fapi/v1/openOrders", signed=True)
            for o in open_orders:
                sym = o.get("symbol", "")
                if sym and sym not in symbols_safe:
                    stale_symbols_regular.add(sym)
            print(f"  🔍 cleanup: regular openOrders={len(open_orders)} total | orphan={sorted(stale_symbols_regular) or 'none'}")
        except Exception as e:
            print(f"  ⚠️  cleanup: gagal fetch openOrders: {e}")

        # ── Ambil algo/conditional orders (TP/SL — endpoint openAlgoOrders) ───
        stale_algo_by_symbol: dict = {}
        try:
            algo_resp = api_get("/fapi/v1/openAlgoOrders", signed=True)
            algo_list = algo_resp if isinstance(algo_resp, list) else algo_resp.get("orders", [])
            for o in algo_list:
                sym     = o.get("symbol", "")
                algo_id = o.get("algoId") or o.get("orderId")
                if sym and algo_id and sym not in symbols_safe:
                    stale_algo_by_symbol.setdefault(sym, []).append(algo_id)
            print(f"  🔍 cleanup: algo orders={len(algo_list)} total | orphan={sorted(stale_algo_by_symbol.keys()) or 'none'}")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"  🔍 cleanup: openAlgoOrders 404 (tidak ada algo order, atau endpoint belum tersedia di akun ini)")
            else:
                print(f"  ⚠️  cleanup algoOrders HTTP error: {e}")
        except Exception as e:
            print(f"  ⚠️  cleanup algoOrders error: {e}")

        all_stale = stale_symbols_regular | set(stale_algo_by_symbol.keys())

        if not all_stale:
            print(f"  ✅ cleanup: tidak ada orphan orders — semua bersih")
            if not dry_run and not silent_if_clean:
                send_telegram_raw("✅ <b>Cleanup Orders</b>\nTidak ada orphan orders — semua bersih.")
            return

        print(f"  {'🔍 DRY-RUN' if dry_run else '🧹'} cleanup: {len(all_stale)} orphan → {sorted(all_stale)}")

        if dry_run:
            # Hanya laporan — tidak cancel apapun
            pending_note = f"\n⚠️ Pending limit dikecualikan: {sorted(symbols_pending_limit)}" if symbols_pending_limit else ""
            send_telegram_raw(
                f"🔍 <b>Orphan Orders Terdeteksi</b>\n"
                f"{'─'*34}\n"
                f"<b>{len(all_stale)}</b> pair punya orders tanpa posisi:\n"
                + "\n".join([f"  ⚠️ {s}" for s in sorted(all_stale)])
                + pending_note + "\n"
                f"{'─'*34}\n"
                f"Ketik <b>/cleanuporders</b> untuk cancel semua orphan."
            )
            return

        # ── Eksekusi cancel (hanya jika dry_run=False, via /cleanuporders) ────
        cancelled_ok  = []
        cancelled_fail = []

        for sym in all_stale:
            sym_ok = True

            # ── Step 1: fetch semua open orders lalu cancel SATU PER SATU ────────
            # Tidak andalkan bulk allOpenOrders — conditional orders (STOP_MARKET,
            # TAKE_PROFIT_MARKET dengan reduceOnly) sering tidak ikut bulk cancel.
            # Cancel per orderId adalah cara paling pasti.
            try:
                all_open = api_get("/fapi/v1/openOrders", {"symbol": sym}, signed=True)
                cancel_ok = 0
                for o in all_open:
                    oid = o.get("orderId")
                    if not oid:
                        continue
                    try:
                        api_delete("/fapi/v1/order", {"symbol": sym, "orderId": oid})
                        cancel_ok += 1
                        time.sleep(0.05)
                    except Exception as ce:
                        print(f"  ⚠️  [{sym}] Gagal cancel orderId {oid}: {ce}")
                        sym_ok = False
                print(f"  🗑️  [{sym}] Per-orderId: {cancel_ok}/{len(all_open)} orders cancelled")
                # Bulk sebagai tambahan (catch-all jika ada order tidak muncul di openOrders)
                try:
                    api_delete("/fapi/v1/allOpenOrders", {"symbol": sym})
                except Exception:
                    pass
            except Exception as fe:
                print(f"  ⚠️  [{sym}] Gagal fetch openOrders: {fe}")
                # Last resort: coba bulk saja
                try:
                    api_delete("/fapi/v1/allOpenOrders", {"symbol": sym})
                    print(f"  🗑️  [{sym}] Bulk cancel (last resort) OK")
                except Exception as be:
                    print(f"  ⚠️  [{sym}] Bulk cancel juga gagal: {be}")
                    sym_ok = False

            # ── Step 2: cancel algo/conditional orders via algoOrder endpoint ─────
            # Beberapa SL/TP dikirim via algo endpoint (terpisah dari openOrders)
            if sym in stale_algo_by_symbol:
                algo_ids = stale_algo_by_symbol[sym]
                ok = 0
                try:
                    api_delete("/fapi/v1/algoOpenOrders", {"symbol": sym})
                    ok = len(algo_ids)
                    print(f"  🗑️  [{sym}] Bulk algo cancel OK ({ok} orders)")
                except Exception:
                    for algo_id in algo_ids:
                        try:
                            api_delete("/fapi/v1/algoOrder", {"symbol": sym, "algoId": algo_id})
                            ok += 1
                            time.sleep(0.05)
                        except Exception as fe:
                            print(f"  ⚠️  [{sym}] Gagal cancel algoId {algo_id}: {fe}")
                            sym_ok = False
                print(f"  🗑️  [{sym}] {ok}/{len(algo_ids)} algo orders cancelled")

            if sym_ok:
                cancelled_ok.append(sym)
            else:
                cancelled_fail.append(sym)

        pending_note = (
            f"\n⏳ Pending limit (tidak disentuh): {sorted(symbols_pending_limit)}"
            if symbols_pending_limit else ""
        )
        fail_note = (
            f"\n⚠️ Gagal cancel: {sorted(cancelled_fail)}"
            if cancelled_fail else ""
        )
        send_telegram_raw(
            f"🧹 <b>Stale Orders Dibersihkan</b>\n"
            f"{'─'*34}\n"
            f"✅ Berhasil cancel: <b>{len(cancelled_ok)}</b> pair\n"
            + "\n".join([f"  🗑️ {s}" for s in sorted(cancelled_ok)])
            + fail_note + pending_note + "\n"
            f"{'─'*34}\n"
            f"Total orphan ditemukan: {len(all_stale)}"
        )

    except Exception as e:
        print(f"  ⚠️  cleanup_stale_orders error: {e}")
        # FIX: sama seperti cleanup_duplicate_position_orders — kalau auto-cleanup
        # (silent_if_clean=True) yang gagal gara-gara 408 testnet flaky, itu bukan
        # actionable, cukup log console. Manual /cleanuporders tetap dikabari.
        if not silent_if_clean:
            try:
                send_telegram_raw(f"❌ <b>Cleanup Orders Gagal</b>\nError: <code>{e}</code>")
            except Exception:
                pass


def algo_post_sl(symbol: str, side: str, trigger_price: float,
                 quantity: float = 0.0,
                 working_type: str = "MARK_PRICE",
                 position_side: str = "BOTH") -> dict:
    """
    Kirim SL order via /fapi/v1/algoOrder (CONDITIONAL STOP_MARKET).

    Strategi: pakai quantity + reduceOnly=true, BUKAN closePosition=true.
    Alasan: closePosition=true memicu -4130 jika sudah ada order sejenis.
    Dengan quantity+reduceOnly tidak ada konflik.
    Dengan quantity+reduceOnly tidak ada konflik, tidak perlu cancel dulu.

    Jika quantity=0 (tidak diketahui), fallback ke closePosition sekali saja
    setelah cancel allOpenOrders (yang memang tersedia).
    """
    filters   = get_lot_filters(symbol)
    tick_size = filters.get("tickSize", 0.01)   # sudah di-koreksi via pricePrecision
    step_size = filters.get("stepSize", 0.001)
    _sl_qty_prec = filters.get("quantityPrecision", None)
    # Derive qty_prec dari stepSize jika tidak tersedia (testnet sering tidak return quantityPrecision)
    if _sl_qty_prec is None and step_size > 0:
        _mkt_step = filters.get("mkt_stepSize", step_size)
        _eff_step = _mkt_step if _mkt_step > 0 else step_size
        _sl_qty_prec = max(0, round(-math.log10(_eff_step)))

    # FIX -4005 / FIX proteksi-parsial: qty > mkt_maxQty TIDAK BOLEH di-clamp
    # (clamp lama memotong SL jadi hanya sebagian posisi, sisanya tanpa
    # proteksi sama sekali — ini penyebab kasus NOTUSDT loss -133 USDT
    # padahal seharusnya max -41 USDT). Sekarang: pecah jadi beberapa order
    # reduceOnly yang totalnya tetap menutup SELURUH posisi.
    _sl_mkt_max = filters.get("mkt_maxQty", filters.get("maxQty", 9999999.0))
    if _sl_mkt_max and quantity > 0 and quantity > _sl_mkt_max:
        print(f"  🔻 algo_post_sl [{symbol}]: qty={quantity} > mkt_maxQty={_sl_mkt_max} → split jadi beberapa order (BUKAN di-clamp)")
        _sl_results = []
        _sl_remaining = quantity
        while _sl_remaining > 0:
            _sl_chunk = min(_sl_remaining, _sl_mkt_max)
            _sl_results.append(
                algo_post_sl(symbol, side, trigger_price, quantity=_sl_chunk,
                             working_type=working_type, position_side=position_side)
            )
            _sl_remaining = round(_sl_remaining - _sl_chunk, 10)
        return _sl_results if len(_sl_results) > 1 else _sl_results[0]

    if _sl_qty_prec is not None:
        quantity = round(math.floor(quantity * (10 ** _sl_qty_prec)) / (10 ** _sl_qty_prec), _sl_qty_prec)
    direction = "LONG" if side == "SELL" else "SHORT"
    trigger_type = "MARK_PRICE" if working_type == "MARK_PRICE" else "CONTRACT_PRICE"

    def _safe_trigger(price: float) -> str:
        """Validasi harga trigger vs mark price, lalu format ke string presisi."""
        try:
            mark = get_current_price(symbol)
            if mark is not None:
                if direction == "LONG" and price >= mark:
                    price = round_price_to_tick(mark * 0.994, tick_size)
                elif direction == "SHORT" and price <= mark:
                    price = round_price_to_tick(mark * 1.006, tick_size)
        except Exception:
            pass
        return format_price(price, tick_size)

    # ── Helper: pasang SL via endpoint standard /fapi/v1/order ──────────────
    def _post_sl_standard(trigger_price: float) -> dict:
        """
        Pasang STOP_MARKET via endpoint standard Binance Futures.
        Lebih kompatibel dari algoOrder — tersedia di semua akun termasuk testnet.
        """
        sl_trigger = _safe_trigger(trigger_price)
        _params = {
            "symbol":           symbol,
            "side":             side,
            "type":             "STOP_MARKET",
            "stopPrice":        sl_trigger,
            "workingType":      working_type,
            "timeInForce":      "GTC",
        }
        if quantity > 0:
            _params["quantity"] = format_qty(quantity, step_size, _sl_qty_prec)
            if position_side != "BOTH":
                _params["positionSide"] = position_side
            else:
                _params["reduceOnly"] = "true"
        else:
            # FIX: closePosition=true di HEDGE mode WAJIB tetap menyertakan
            # positionSide (lihat penjelasan di _emergency_close di bawah) —
            # tanpa ini Binance reject 400.
            _params["closePosition"] = "true"
            if position_side != "BOTH":
                _params["positionSide"] = position_side
        r = api_post("/fapi/v1/order", _params)
        return r

    # ── Coba algoOrder dulu, fallback ke standard jika tidak support ─────────
    if quantity > 0:
        buffers = [0.000, 0.005, 0.010, 0.015]
        algo_supported = True   # asumsi support, akan di-set False jika -4120
        for i, _buf in enumerate(buffers):
            try:
                mark = get_current_price(symbol) or trigger_price
                adj_price = trigger_price
                if _buf > 0:
                    adj_price = adjust_sl_price(trigger_price, mark, direction, tick_size, buffer_pct=_buf)
                sl_trigger = _safe_trigger(adj_price)

                if not algo_supported:
                    # AlgoOrder tidak support → langsung pakai standard
                    return _post_sl_standard(adj_price)

                _sl_params = {
                    "algoType":     "CONDITIONAL",
                    "symbol":       symbol,
                    "side":         side,
                    "type":         "STOP_MARKET",
                    "triggerPrice": sl_trigger,
                    "triggerType":  trigger_type,
                    "quantity":     format_qty(quantity, step_size, _sl_qty_prec),
                }
                if position_side != "BOTH":
                    _sl_params["positionSide"] = position_side
                else:
                    _sl_params["reduceOnly"] = "true"
                params = sign_request(_sl_params)
                r = requests.post(get_base_url() + "/fapi/v1/algoOrder", params=params, headers=get_headers())
                if r.status_code == 200:
                    return r.json()
                err = r.json() if "application/json" in r.headers.get("Content-Type", "") else {"code": 0, "msg": r.text}
                code = err.get("code", 0)
                msg  = str(err.get("msg", r.text))
                # -4120 = algoOrder tidak support di akun ini → fallback ke standard
                if code in (-4120, -1121, -4003):
                    print(f"  ⚠️  algoOrder tidak support [{symbol}] (code {code}) — fallback ke STOP_MARKET standard")
                    algo_supported = False
                    return _post_sl_standard(adj_price)
                # ── FIX: algoOrder punya cap quantity tersendiri (tidak selalu sama
                # dengan MARKET_LOT_SIZE.maxQty di exchangeInfo) — misal TAOUSDT
                # qty=29 ditolak "Quantity greater than max quantity." padahal
                # mkt_maxQty tercatat jauh lebih besar. Tanpa fix ini, bot retry
                # ribuan kali dengan quantity yang SAMA dan tidak akan pernah
                # berhasil (deadlock), sementara posisi tetap tanpa SL.
                # Solusi: shrink quantity jadi setengah dan split jadi 2 order
                # reduceOnly terpisah yang totalnya tetap menutup seluruh posisi.
                if "max quantity" in msg.lower():
                    _min_step = step_size if step_size > 0 else 0.001
                    if quantity > _min_step * 2:
                        half_qty = quantity / 2
                        if _sl_qty_prec is not None:
                            half_qty = round(math.floor(half_qty * (10 ** _sl_qty_prec)) / (10 ** _sl_qty_prec), _sl_qty_prec)
                        if half_qty <= 0:
                            half_qty = _min_step
                        print(f"  🔻 algo_post_sl [{symbol}]: qty={quantity} ditolak (max quantity) → split jadi {half_qty} + sisanya")
                        first = algo_post_sl(symbol, side, trigger_price, quantity=half_qty,
                                              working_type=working_type, position_side=position_side)
                        remaining_qty = round(quantity - half_qty, 10)
                        if remaining_qty > 0:
                            try:
                                second = algo_post_sl(symbol, side, trigger_price, quantity=remaining_qty,
                                                       working_type=working_type, position_side=position_side)
                                return [first, second]
                            except Exception as _split_e:
                                print(f"  ⚠️  algo_post_sl [{symbol}]: sisa {remaining_qty} gagal dipasang ({_split_e}) — sebagian posisi tetap terlindungi")
                                return first
                        return first
                    # Qty sudah sekecil mungkin dan tetap ditolak → tidak ada gunanya
                    # terus mencoba qty yang sama, biarkan error naik ke pemanggil.
                    raise RuntimeError(f"algoOrder [{symbol}] menolak quantity minimum ({quantity}): {msg}")
                print(f"  ⚠️  algoOrder {r.status_code} [{symbol}] attempt {i}: {err.get('msg', r.text)}")
                if i < len(buffers) - 1:
                    time.sleep(0.3)
                    continue
                # Semua buffer habis → coba standard sebagai last resort
                print(f"  ⚠️  algoOrder semua buffer gagal [{symbol}] — fallback ke standard")
                return _post_sl_standard(trigger_price)
            except Exception as e:
                if i < len(buffers) - 1:
                    print(f"  ⚠️  algoOrder exception [{symbol}] attempt {i}: {e}")
                    time.sleep(0.3)
                else:
                    print(f"  ⚠️  algoOrder exception final [{symbol}]: {e} — fallback ke standard")
                    return _post_sl_standard(trigger_price)
        raise RuntimeError(f"algo_post_sl gagal [{symbol}]")

    # ── Fallback: quantity tidak diketahui → standard closePosition ──────────
    return _post_sl_standard(trigger_price)


def algo_post_tp(symbol: str, side: str, trigger_price: float, quantity: float,
                 working_type: str = "MARK_PRICE",
                 position_side: str = "BOTH") -> dict:
    """
    Pasang TP order via /fapi/v1/algoOrder (CONDITIONAL TAKE_PROFIT_MARKET).

    Perbaikan vs versi lama:
    1. Fallback ke standard /fapi/v1/order jika algoOrder tidak support (-4120, testnet)
    2. Re-sign SETELAH update triggerPrice (bukan sebelum) — cegah stale params
    3. Pakai mkt_stepSize untuk format_qty (TP adalah market order saat trigger)
    4. Guard: quantity tidak boleh < min_qty setelah floor
    """
    trigger_type  = "MARK_PRICE" if working_type == "MARK_PRICE" else "CONTRACT_PRICE"
    filters       = get_lot_filters(symbol)
    tick_size     = filters.get("tickSize",    0.01)
    step_size     = filters.get("stepSize",    0.001)
    mkt_step_size = filters.get("mkt_stepSize", step_size)   # FIX: pakai market lot step
    if mkt_step_size <= 0:
        mkt_step_size = step_size
    min_qty       = filters.get("minQty",      0.001)
    _tp_qty_prec  = filters.get("quantityPrecision", None)
    # Derive qty_prec dari mkt_stepSize jika tidak tersedia (testnet sering tidak return quantityPrecision)
    if _tp_qty_prec is None and mkt_step_size > 0:
        _tp_qty_prec = max(0, round(-math.log10(mkt_step_size)))

    # FIX -4005 / FIX proteksi-parsial: qty > mkt_maxQty TIDAK BOLEH di-clamp
    # (clamp lama memotong TP jadi hanya sebagian posisi, sisanya tanpa TP
    # sama sekali). Sekarang: pecah jadi beberapa order reduceOnly yang
    # totalnya tetap menutup SELURUH quantity yang diminta.
    _tp_mkt_max = filters.get("mkt_maxQty", filters.get("maxQty", 9999999.0))
    if _tp_mkt_max and quantity > _tp_mkt_max:
        print(f"  🔻 algo_post_tp [{symbol}]: qty={quantity} > mkt_maxQty={_tp_mkt_max} → split jadi beberapa order (BUKAN di-clamp)")
        _tp_results = []
        _tp_remaining = quantity
        while _tp_remaining > 0:
            _tp_chunk = min(_tp_remaining, _tp_mkt_max)
            _tp_results.append(
                algo_post_tp(symbol, side, trigger_price, quantity=_tp_chunk,
                             working_type=working_type, position_side=position_side)
            )
            _tp_remaining = round(_tp_remaining - _tp_chunk, 10)
        return _tp_results if len(_tp_results) > 1 else _tp_results[0]

    # Floor quantity ke presisi exchange
    if _tp_qty_prec is not None:
        quantity = round(math.floor(quantity * (10 ** _tp_qty_prec)) / (10 ** _tp_qty_prec), _tp_qty_prec)

    # Guard: quantity tidak boleh 0 atau di bawah minQty
    if quantity < min_qty:
        quantity = round_lot_to_step(min_qty, mkt_step_size)
        if _tp_qty_prec is not None:
            quantity = round(math.floor(quantity * (10 ** _tp_qty_prec)) / (10 ** _tp_qty_prec), _tp_qty_prec)

    direction_str = "LONG" if side == "SELL" else "SHORT"  # side = sisi close

    # ── Helper: pasang TP via standard endpoint (fallback jika algoOrder tidak support) ──
    def _post_tp_standard(tp_price: float) -> dict:
        """
        Pasang TAKE_PROFIT_MARKET via /fapi/v1/order standard.
        Dipakai saat algoOrder tidak support (testnet, akun tanpa fitur algo).
        """
        _p = {
            "symbol":      symbol,
            "side":        side,
            "type":        "TAKE_PROFIT_MARKET",
            "stopPrice":   format_price(tp_price, tick_size),
            "workingType": working_type,
            "timeInForce": "GTC",
            "quantity":    format_qty(quantity, mkt_step_size, _tp_qty_prec),
        }
        if position_side != "BOTH":
            _p["positionSide"] = position_side
        else:
            _p["reduceOnly"] = "true"
        return api_post("/fapi/v1/order", _p)

    # ── Coba algoOrder dulu, fallback ke standard jika -4120 ─────────────────
    algo_supported = True
    buffers        = [0.000, 0.003, 0.006, 0.010]
    last_err       = None

    for _i, _buf in enumerate(buffers):
        try:
            # Hitung adjusted trigger price (sebelum sign!)
            adj_price = trigger_price
            if _buf > 0:
                mark      = get_current_price(symbol) or trigger_price
                adj_price = adjust_tp_price(trigger_price, mark, direction_str, tick_size, buffer_pct=_buf)

            if not algo_supported:
                # AlgoOrder tidak support → langsung fallback ke standard
                return _post_tp_standard(adj_price)

            _tp_params = {
                "algoType":     "CONDITIONAL",
                "symbol":       symbol,
                "side":         side,
                "type":         "TAKE_PROFIT_MARKET",
                "triggerPrice": format_price(adj_price, tick_size),   # FIX: sign SETELAH update
                "triggerType":  trigger_type,
                "quantity":     format_qty(quantity, mkt_step_size, _tp_qty_prec),
            }
            if position_side != "BOTH":
                _tp_params["positionSide"] = position_side
            else:
                _tp_params["reduceOnly"] = "true"

            # FIX: sign params SETELAH triggerPrice diupdate, bukan sebelumnya
            params = sign_request(_tp_params)
            _r = requests.post(get_base_url() + "/fapi/v1/algoOrder", params=params, headers=get_headers())

            if _r.status_code == 200:
                return _r.json()

            last_err = _r.json() if "application/json" in _r.headers.get("Content-Type", "") else {"msg": _r.text}
            code     = last_err.get("code", 0)
            _tp_msg  = str(last_err.get("msg", _r.text))

            # -4120 / -1121 / -4003 = algoOrder tidak support → fallback ke standard
            if code in (-4120, -1121, -4003):
                print(f"  ⚠️  algo_post_tp: algoOrder tidak support [{symbol}] (code {code}) — fallback ke standard TAKE_PROFIT_MARKET")
                algo_supported = False
                return _post_tp_standard(adj_price)

            # ── FIX: sama seperti algo_post_sl — algoOrder punya cap quantity
            # sendiri yang tidak selalu tercermin di mkt_maxQty exchangeInfo.
            # Tanpa fix ini, retry berulang dengan quantity yang sama tidak
            # akan pernah berhasil. Shrink + split jadi 2 order reduceOnly.
            if "max quantity" in _tp_msg.lower():
                _min_step = mkt_step_size if mkt_step_size > 0 else 0.001
                if quantity > _min_step * 2:
                    half_qty = quantity / 2
                    if _tp_qty_prec is not None:
                        half_qty = round(math.floor(half_qty * (10 ** _tp_qty_prec)) / (10 ** _tp_qty_prec), _tp_qty_prec)
                    if half_qty <= 0:
                        half_qty = _min_step
                    print(f"  🔻 algo_post_tp [{symbol}]: qty={quantity} ditolak (max quantity) → split jadi {half_qty} + sisanya")
                    first = algo_post_tp(symbol, side, trigger_price, quantity=half_qty,
                                          working_type=working_type, position_side=position_side)
                    remaining_qty = round(quantity - half_qty, 10)
                    if remaining_qty > 0:
                        try:
                            second = algo_post_tp(symbol, side, trigger_price, quantity=remaining_qty,
                                                   working_type=working_type, position_side=position_side)
                            return [first, second]
                        except Exception as _split_e:
                            print(f"  ⚠️  algo_post_tp [{symbol}]: sisa {remaining_qty} gagal dipasang ({_split_e}) — sebagian TP tetap terpasang")
                            return first
                    return first
                raise RuntimeError(f"algoOrder [{symbol}] menolak quantity minimum TP ({quantity}): {_tp_msg}")

            print(f"  ⚠️  algo_post_tp {_r.status_code} [{symbol}] attempt {_i}: {last_err.get('msg', _r.text)}")
            if _i < len(buffers) - 1:
                time.sleep(0.3)
                continue
            # Semua buffer habis → coba standard sebagai last resort
            print(f"  ⚠️  algo_post_tp semua buffer gagal [{symbol}] — fallback ke standard")
            return _post_tp_standard(trigger_price)

        except Exception as _te:
            if _i < len(buffers) - 1:
                time.sleep(0.3)
                continue
            # Last attempt gagal → coba standard satu kali lagi sebelum raise
            try:
                print(f"  ⚠️  algo_post_tp exception [{symbol}] attempt {_i}: {_te} — fallback ke standard")
                return _post_tp_standard(trigger_price)
            except Exception as _se:
                raise RuntimeError(f"algo_post_tp gagal total [{symbol}]: algoOrder={_te} standard={_se}")

    raise RuntimeError(f"algo_post_tp gagal [{symbol}]: {last_err}")


def check_order_status_direct(symbol: str, order_ref: dict) -> str | None:
    """
    Cek status order LANGSUNG ke Binance pakai ID order yang tersimpan saat
    order dipasang — bukan menebak dari kombinasi harga/lot seperti sebelumnya.

    order_ref: dict hasil return algo_post_tp()/algo_post_sl() (atau list kalau
    order sempat displit jadi 2 karena max-quantity — lihat algo_post_tp/sl).

    Return salah satu dari:
      "FILLED"     → order benar-benar sudah dieksekusi Binance
      "NEW"        → order masih menunggu, belum kena
      "CANCELED"/"EXPIRED" → order sudah tidak aktif tanpa fill
      None         → gagal cek (network error dll), caller harus retry nanti,
                      JANGAN diasumsikan FILLED atau NEW.

    Kalau order_ref adalah list (hasil split qty), FILLED hanya jika SEMUA
    bagian FILLED; kalau salah satu belum, dianggap belum lengkap ("NEW").
    """
    if not order_ref:
        return None

    refs = order_ref if isinstance(order_ref, list) else [order_ref]
    statuses = []
    for ref in refs:
        if not isinstance(ref, dict):
            statuses.append(None)
            continue
        algo_id = ref.get("algoId")
        order_id = ref.get("orderId")
        try:
            if algo_id:
                # Order via /fapi/v1/algoOrder → query statusnya lewat endpoint yang sama
                resp = api_get("/fapi/v1/algoOrder", {"symbol": symbol, "algoId": algo_id}, signed=True)
                st = str(resp.get("algoStatus", "")).upper()
                # algoStatus Binance: NEW / TRIGGERED / FINISHED / CANCELED / EXPIRED
                # "TRIGGERED"/"FINISHED" artinya sudah tereksekusi
                if st in ("FINISHED", "TRIGGERED"):
                    statuses.append("FILLED")
                elif st in ("CANCELED", "EXPIRED"):
                    statuses.append(st)
                else:
                    statuses.append("NEW")
            elif order_id:
                # Order via /fapi/v1/order standard (fallback non-algo)
                resp = api_get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
                st = str(resp.get("status", "")).upper()
                if st == "FILLED":
                    statuses.append("FILLED")
                elif st in ("CANCELED", "EXPIRED", "REJECTED"):
                    statuses.append(st)
                else:
                    statuses.append("NEW")
            else:
                statuses.append(None)
        except Exception as e:
            print(f"  ⚠️  check_order_status_direct [{symbol}] gagal query order: {e}")
            statuses.append(None)

    if any(s is None for s in statuses):
        return None  # salah satu gagal dicek → jangan simpulkan apa-apa
    if all(s == "FILLED" for s in statuses):
        return "FILLED"
    if any(s in ("CANCELED", "EXPIRED", "REJECTED") for s in statuses):
        return "CANCELED"
    return "NEW"


def get_contract_multiplier(symbol: str) -> int:
    """
    Pair dengan prefix '1000' di Binance Futures artinya 1 kontrak = 1000 unit token.
    markPrice yang dikembalikan Binance adalah harga per unit token (sangat kecil),
    tapi lot/qty yang dikirim ke order adalah dalam satuan kontrak.
    Jadi notional sesungguhnya = qty_kontrak × markPrice × multiplier.
    Contoh: 1000PEPEUSDT → multiplier=1000
            1000SHIBUSDT → multiplier=1000
    Pair normal → multiplier=1
    """
    if symbol.startswith("1000"):
        return 1000
    return 1


# Cache harga mark per symbol — TTL 20 detik, cukup untuk scan & micro-price check.
# Mencegah 58+ API calls berturut-turut setiap scan saat process_pair dipanggil paralel.
_price_cache: dict = {}   # symbol -> (price, timestamp)
_PRICE_CACHE_TTL = 20     # detik

def get_current_price(symbol: str) -> float | None:
    import time as _t
    now = _t.time()
    cached = _price_cache.get(symbol)
    if cached:
        price_val, ts = cached
        if now - ts < _PRICE_CACHE_TTL:
            return price_val
    try:
        data = api_get("/fapi/v1/premiumIndex", {"symbol": symbol})
        price = float(data["markPrice"])
        _price_cache[symbol] = (price, now)
        return price
    except Exception:
        try:
            data = api_get("/fapi/v1/ticker/price", {"symbol": symbol})
            price = float(data["price"])
            _price_cache[symbol] = (price, now)
            return price
        except Exception:
            return None


def get_actual_close_price(symbol: str, lookback_sec: int = 300) -> float | None:
    """
    Ambil harga FILL ASLI dari trade terakhir yang menutup posisi symbol ini,
    langsung dari /fapi/v1/userTrades — BUKAN mark price / ticker price live.

    KENAPA INI PENTING: get_current_price() mengambil harga PASAR SAAT INI,
    yang bisa sudah bergerak jauh dari harga eksekusi order close yang
    sebenarnya (kasus nyata: SL kena di 82.53 dengan slippage sampai fill di
    ~81.5 karena likuiditas tipis / gerakan cepat, tapi begitu bot baru sempat
    deteksi & cek harga beberapa detik kemudian, harga sudah rebound ke 83.38
    — notifikasi jadi salah total, melaporkan "profit" padahal sebenarnya rugi
    kena SL dengan slippage).

    Return average weighted fill price dari semua trade closing dalam
    lookback_sec detik terakhir, atau None kalau gagal / tidak ketemu.
    """
    try:
        since_ms = int((time.time() - lookback_sec) * 1000)
        trades = api_get(
            "/fapi/v1/userTrades",
            {"symbol": symbol, "startTime": since_ms, "limit": 50},
            signed=True,
        )
        if not trades:
            return None
        # Filter hanya trade yang sifatnya menutup posisi (mengurangi size).
        closing = [t for t in trades if bool(t.get("reduceOnly") or t.get("maker") is not None)]
        candidates = closing if closing else trades
        total_qty = sum(abs(float(t.get("qty", 0))) for t in candidates)
        if total_qty <= 0:
            return None
        weighted = sum(float(t.get("price", 0)) * abs(float(t.get("qty", 0))) for t in candidates)
        return weighted / total_qty
    except Exception as e:
        print(f"  ⚠️  get_actual_close_price gagal [{symbol}]: {e}")
        return None


def adjust_sl_price(sl: float, mark_price: float, direction: str, tick_size: float,
                    buffer_pct: float = 0.005) -> float:
    min_ticks = max(1, math.ceil(mark_price * buffer_pct / tick_size))
    if direction == "LONG":
        ceiling = round_price_to_tick(mark_price - tick_size * min_ticks, tick_size)
        if sl >= ceiling:
            sl = ceiling
    else:
        floor_ = round_price_to_tick(mark_price + tick_size * min_ticks, tick_size)
        if sl <= floor_:
            sl = floor_
    return round_price_to_tick(sl, tick_size)


def adjust_tp_price(tp: float, mark_price: float, direction: str, tick_size: float,
                    buffer_pct: float = 0.002) -> float:
    min_ticks = max(1, math.ceil(mark_price * buffer_pct / tick_size))
    if direction == "LONG":
        floor_ = round_price_to_tick(mark_price + tick_size * min_ticks, tick_size)
        if tp < floor_:
            tp = floor_
    else:
        ceiling = round_price_to_tick(mark_price - tick_size * min_ticks, tick_size)
        if tp > ceiling:
            tp = ceiling
    return round_price_to_tick(tp, tick_size)


def _trades_today_roll():
    """Reset hitungan kalau sudah ganti hari UTC. Panggil di dalam lock."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _trades_today_state.get("date") != today:
        _trades_today_state["date"]  = today
        _trades_today_state["count"] = 0


def get_trades_today() -> int:
    """Jumlah trade yang sudah TERISI hari ini (UTC)."""
    with _trades_today_lock:
        _trades_today_roll()
        return int(_trades_today_state["count"])


def register_trade_filled(symbol: str = ""):
    """Catat 1 trade terisi. Dipanggil sekali per posisi baru dari limit fill."""
    with _trades_today_lock:
        _trades_today_roll()
        _trades_today_state["count"] += 1
        n = _trades_today_state["count"]
    lim = f"/{MAX_TRADES_PER_DAY}" if MAX_TRADES_PER_DAY > 0 else ""
    print(f"  🧮 Trade hari ini: {n}{lim} ({symbol})")
    save_state()


def daily_trade_limit_reached() -> tuple:
    """(reached, filled, pending, limit). Pending ikut dihitung supaya limit order
    yang belum terisi tidak bisa meloloskan trade melebihi batas."""
    if MAX_TRADES_PER_DAY <= 0:
        return False, get_trades_today(), len(pending_limit_orders), 0
    filled  = get_trades_today()
    pending = len(pending_limit_orders)
    return (filled + pending) >= MAX_TRADES_PER_DAY, filled, pending, MAX_TRADES_PER_DAY


# ── Cooldown notifikasi SKIP ENTRY (anti-spam Telegram) ─────────────────────
_skip_notif_last = {}          # {(symbol, reason): timestamp}
SKIP_NOTIF_COOLDOWN = 1800     # detik (30 menit) per simbol per alasan

def _notify_skip_entry(symbol: str, reason: str, text: str):
    """Kirim Telegram SKIP ENTRY maksimal 1x per SKIP_NOTIF_COOLDOWN per (symbol, reason)."""
    key = (symbol, reason)
    now = time.time()
    if now - _skip_notif_last.get(key, 0) < SKIP_NOTIF_COOLDOWN:
        return
    _skip_notif_last[key] = now
    send_telegram_raw(text)


def execute_trade(signal: dict, mode: dict) -> bool:
    """
    Return True  → limit order berhasil dikirim ke Binance.
    Return False → skip (silent: posisi sudah ada, slot penuh, paused, dll).
    Raise        → error saat proses (notif Telegram sudah dikirim di dalam).
    """
    if not AUTO_TRADING:
        return False
    if bot_paused:
        print("⏸ Bot paused — skip trade")
        return False  # no tele notif for paused (too frequent)

    symbol  = signal["pair"]
    side    = "BUY" if signal["direction"] == "LONG" else "SELL"
    sl_side = "SELL" if side == "BUY" else "BUY"
    direction = signal["direction"]

    if has_position(symbol):
        print("⛔ Already in position")
        return False  # silent: normal dedup

    # Guard tambahan: cek pending limit order untuk pair yang sama
    # (bisa dari Pre-Zone Sniper yang sudah pasang order sebelumnya)
    if symbol in pending_limit_orders:
        pend_dir = pending_limit_orders[symbol].get("direction", "")
        print(f"⛔ [{symbol}] Sudah ada pending limit order {pend_dir} — skip double entry")
        return False

    dynamic_max = get_dynamic_max_trades()
    current_open = count_open_positions()
    if current_open >= dynamic_max:
        print(f"⛔ Max trades reached ({current_open}/{dynamic_max})")
        return False  # silent: normal capacity limit

    # ── Batas trade per hari (/maxtradesperday) ──────────────────────────────
    _lim_hit, _lim_filled, _lim_pend, _lim_max = daily_trade_limit_reached()
    if _lim_hit:
        print(f"⛔ [{symbol}] Max trade per hari tercapai "
              f"(terisi {_lim_filled} + pending {_lim_pend} >= {_lim_max}) — skip")
        return False  # silent: batas harian, notif sekali di bawah

    balance  = get_total_balance()   # pakai total balance (konsisten, tidak turun tiap trade buka)
    # FIX: jika balance None (API error / IP ban), skip entry — jangan crash atau pakai 0
    if not balance:
        print(f"⛔ [{symbol}] get_total_balance() error/None — skip entry (API tidak bisa dijangkau)")
        return False

    # ── Ambil filter Binance lebih dulu (dibutuhkan untuk kalkulasi leverage) ──
    _pre_filters      = get_lot_filters(symbol)
    _pre_min_notional = _pre_filters.get("minNotional", 5.0)

    available_bal = get_balance()
    # FIX: handle None dari get_balance (return None saat error, bukan 0)
    if available_bal is None:
        print(f"⛔ [{symbol}] get_balance() gagal (API error) — skip")
        _notify_skip_entry(symbol, "api_error",
            f"⛔ <b>SKIP ENTRY — {symbol}</b>\nGagal mengambil balance dari Binance (API error).")
        return False
    if available_bal <= 0:
        print(f"⛔ [{symbol}] available balance=0 (margin habis terpakai posisi/pending order) — skip")
        _notify_skip_entry(symbol, "no_margin",
            f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
            f"Margin habis: available balance = 0 USDT "
            f"(terpakai posisi aktif + limit order pending).")
        return False

    # ── MAX SL LOSS: Lot disesuaikan agar loss jika SL kena ≤ MAX_SL_LOSS_PCT × balance ──
    #
    # Logika baru (SL-based sizing):
    #   1. max_loss_usdt = MAX_SL_LOSS_PCT × total_balance
    #      (misal 1% × $10.000 = $100 — ini kerugian MAKSIMAL jika SL kena)
    #   2. sl_distance = |entry - stop_loss|  (dalam unit harga)
    #   3. lot = max_loss_usdt / sl_distance
    #      → berapa unit agar loss aktual saat SL kena = max_loss_usdt
    #   4. Lot di-clamp ke exchange limits dan cap notional
    #
    # Tujuan: berapa pun jarak SL-nya, LOSS MAKSIMAL TETAP = X% dari balance.
    # Lot otomatis menyesuaikan: SL dekat → lot besar, SL jauh → lot kecil.
    _entry       = signal["entry"]
    _sl          = signal["stop_loss"]
    _sl_dist_abs = abs(_entry - _sl)
    _sl_dist_pct = _sl_dist_abs / max(_entry, 1e-9)


    # ── Tentukan leverage ─────────────────────────────────────────────────────
    if symbol in PER_SYMBOL_LEVERAGE:
        trade_leverage = PER_SYMBOL_LEVERAGE[symbol]
        print(f"  ⚡ [{symbol}] Per-symbol leverage override: {trade_leverage}x")
    elif FIXED_LEVERAGE > 0:
        trade_leverage = FIXED_LEVERAGE
    else:
        trade_leverage = get_leverage_for_price(_entry, symbol=symbol)

    # ── Hitung lot dari max SL loss ───────────────────────────────────────────
    # lot = max_loss_usdt / sl_dist  →  jika SL kena, loss = lot × sl_dist = max_loss_usdt
    # Leverage tidak mempengaruhi loss saat SL kena, hanya mempengaruhi margin.
    # ── FIX -2019: Hitung pending margin SEBELUM lot sizing ─────────────────
    # Pending LIMIT orders sudah lock margin di Binance meski belum terisi.
    # get_balance() bisa stale saat banyak sinyal masuk hampir bersamaan —
    # setiap sinyal dapat nilai yg sama → semua over-commit margin → -2019.
    # Solusi: deduct margin pending orders PENUH (bukan 50%) sebelum sizing.
    _pending_margin_used = 0.0
    for _pend_sym, _pend in pending_limit_orders.items():
        if _pend_sym == symbol:
            continue
        try:
            _p_notional = (_pend.get("lot", 0) or 0) * (_pend.get("entry", 0) or 0)
            _p_lev      = PER_SYMBOL_LEVERAGE.get(_pend_sym, trade_leverage)
            _pending_margin_used += _p_notional / max(_p_lev, 1)
        except Exception:
            pass
    _effective_available = max(0.0, available_bal - _pending_margin_used)
    if _pending_margin_used > 0:
        print(f"  ℹ️  [{symbol}] Margin pending orders: {_pending_margin_used:.2f} USDT → effective avail: {_effective_available:.2f} USDT")

    # ── FIX -2019: Sizing dari _effective_available, bukan total_balance ─────
    # Sebelumnya: max_loss_usdt = balance (total) * MAX_SL_LOSS_PCT
    # → lot terlalu besar untuk available balance → margin req > avail → -2019
    # Sekarang: pakai available_bal dikurangi pending margin → lot realistic
    #
    # FIX: streak_mult (lose_streak≥3 → risk 0.5x) DIHAPUS atas permintaan
    # user — balance sudah otomatis mengecil kalau lagi lose streak, jadi
    # max_loss_usdt = _effective_available × MAX_SL_LOSS_PCT sudah otomatis
    # ikut turun tanpa multiplier tambahan. Streak_mult sebelumnya jadi
    # pengurangan GANDA yang bikin realisasi loss jatuh jauh di bawah target
    # /setmarginratio (mis. target 1%=$50 tapi realisasi cuma ~$25 karena
    # 0.5x, lalu masih kepotong lot-rounding lagi jadi $15).
    max_loss_usdt = _effective_available * MAX_SL_LOSS_PCT
    min_sl_dist   = _entry * MIN_SL_DISTANCE_PCT
    eff_sl_dist   = max(_sl_dist_abs, min_sl_dist)
    raw_lot       = max_loss_usdt / max(eff_sl_dist, 1e-9)
    # ── Hard cap margin per trade: max = effective_available * MARGIN_SAFETY_FACTOR ──
    # Ini mencegah lot raksasa saat SL distance sangat kecil (misal BTC SL 0.2%).
    # Tanpa ini: raw_lot = max_loss/sl_dist bisa sangat besar → notional >> available.
    # MARGIN_SAFETY_FACTOR=0.95 → margin 1 trade max 95% dari effective_available.
    _max_margin_per_trade = _effective_available * MARGIN_SAFETY_FACTOR
    _max_notional_hard    = _max_margin_per_trade * trade_leverage
    raw_lot = min(raw_lot, _max_notional_hard / max(_entry, 1e-9))
    print(f"  💰 [{symbol}] Sizing: avail={available_bal:.2f} - pending={_pending_margin_used:.2f} = eff={_effective_available:.2f} USDT | max_loss={max_loss_usdt:.2f} | margin_cap={_max_margin_per_trade:.2f} | notional_cap={_max_notional_hard:.2f}")

    # ── Ambil filter exchange ─────────────────────────────────────────────────
    # FIX ROOT CAUSE (DOGEUSDT entry/SL/TP collapse ke 0.1/0.0): endpoint
    # /fapi/v1/exchangeInfo TIDAK mendukung query param "symbol" — Binance
    # mengabaikan param ini dan selalu mengembalikan filter SEMUA simbol di
    # exchange. Kode lama asal ambil sym_list[0] (simbol pertama di seluruh
    # response, misal BTCUSDT) dan menimpa _exchange_info_cache[symbol] dengan
    # filter simbol LAIN itu — tickSize/minPrice DOGEUSDT jadi punya BTCUSDT
    # (tickSize 0.10, cocok persis dengan clamp "ke 0.1" yang muncul di log).
    # FIX: cari entry yang symbol-nya benar-benar cocok sebelum dipakai.
    try:
        rt_info = api_get("/fapi/v1/exchangeInfo", {"symbol": symbol})
        sym_list = [s for s in rt_info.get("symbols", []) if s.get("symbol") == symbol]
        if sym_list:
            rt_filters = _parse_symbol_filters(sym_list[0])
            if rt_filters:
                _exchange_info_cache[symbol] = rt_filters
        else:
            print(f"  ⚠️  Real-time filter fetch: {symbol} tidak ditemukan di response exchangeInfo — pakai cache")
    except Exception as _rte:
        print(f"  ⚠️  Real-time filter fetch gagal {symbol}: {_rte} — pakai cache")

    filters       = get_lot_filters(symbol)
    step_size     = filters.get("stepSize",    1.0)
    mkt_step_size = filters.get("mkt_stepSize", step_size)
    if mkt_step_size <= 0:
        mkt_step_size = step_size
    min_qty       = filters.get("minQty",      1.0)
    min_notional  = filters.get("minNotional", 5.0)
    tick_size     = filters.get("tickSize",    0.0001)
    qty_prec      = filters.get("quantityPrecision", None)
    # lot_max = LOT_SIZE.maxQty saja — MARKET_LOT_SIZE.maxQty (mkt_maxQty) TIDAK
    # dipakai untuk sizing lot LIMIT entry. mkt_maxQty hanya untuk clamp SL/TP/close.
    # Ini fix root cause CHZUSDT: mkt_maxQty=1000 tidak lagi kontaminasi lot sizing.
    lot_max       = filters.get("lot_maxQty",  filters.get("maxQty", 9999999.0))

    _cmult = get_contract_multiplier(symbol)

    # ── Feasibility check: hanya skip jika benar-benar mustahil ──────────────
    # Pair mustahil di-trade hanya jika: bahkan dengan max leverage (125x) dan
    # lot minimum (minQty), loss jika SL kena MASIH di atas max_loss_usdt DAN
    # notional MASIH di bawah minNotional sekaligus.
    # Semua skenario lain bisa diselesaikan oleh eskalasi leverage + lot adjustment
    # di blok sizing di bawah.
    _max_possible_lot = max_loss_usdt / max(eff_sl_dist, 1e-9)   # batas SL
    if (_max_possible_lot * _entry * _cmult) < min_notional:
        # Bahkan dengan lot maksimum yang diizinkan risk → notional masih kurang
        _msg = (f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Tidak bisa entry: max lot dari risk={_max_possible_lot:.2f} × "
                f"entry={_entry:.6f} = {_max_possible_lot*_entry:.2f} &lt; minNotional={min_notional}\n"
                f"Naikkan /setmarginratio atau perbesar balance.")
        print(f"⛔ [{symbol}] max_possible_notional={_max_possible_lot*_entry:.2f} < minNotional={min_notional}, skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return False

    # ── Clamp lot ke SL-based cap (hard limit risk) ───────────────────────────
    # Hanya cap berdasarkan risk (SL loss) — bukan maxQty exchange.
    # Clamp ke lot_maxQty dilakukan tepat sebelum kirim LIMIT order.
    _sl_cap_lot = max_loss_usdt / max(eff_sl_dist, 1e-9)
    raw_lot = min(raw_lot, _sl_cap_lot)

    # ── Handle minNotional: naikkan lot sampai cukup, lalu eskalasi leverage ──
    # Urutan prioritas:
    # 1. Naikkan lot sampai notional >= minNotional, selama SL loss masih aman.
    # 2. Jika lot sudah di batas SL cap tapi notional masih kurang → eskalasi leverage
    #    (leverage tidak ubah notional tapi mungkin unlock lot lebih besar via margin).
    # 3. Jika semua opsi habis → skip dengan penjelasan.
    target_notional = raw_lot * _entry
    if target_notional < min_notional:
        # Coba naikkan lot langsung ke minimum yang penuhi minNotional
        _lot_for_notional = min_notional / max(_entry, 1e-9)
        _loss_for_notional = _lot_for_notional * eff_sl_dist
        if _loss_for_notional <= max_loss_usdt:
            # Aman — lot baru masih dalam batas SL loss
            raw_lot = _lot_for_notional
            target_notional = raw_lot * _entry
            print(f"  ℹ️  [{symbol}] lot naik ke {raw_lot:.4f} agar penuhi minNotional={min_notional:.2f}")
        elif FIXED_LEVERAGE <= 0:
            # Lot minimum untuk minNotional terlalu besar dari sisi SL loss.
            # Coba eskalasi leverage — leverage lebih tinggi = margin lebih kecil per lot
            # sehingga dengan modal yang sama bisa hold lot lebih besar.
            for _try_lev in [x for x in [10, 15, 20, 25, 30, 50, 75, 100, 125] if x > trade_leverage]:
                # Dengan leverage lebih tinggi, margin per lot lebih kecil.
                # Kalkulasi ulang lot maksimum berdasarkan margin tersedia dengan leverage baru.
                _try_margin_cap = _effective_available * MARGIN_SAFETY_FACTOR * _try_lev
                _try_lot = min(_try_margin_cap / max(_entry, 1e-9), _sl_cap_lot)
                _try_lot = max(_try_lot, _lot_for_notional)   # minimal yang cukup notional
                _try_notional = _try_lot * _entry
                _try_loss = _try_lot * eff_sl_dist
                if _try_notional >= min_notional and _try_loss <= max_loss_usdt:
                    trade_leverage  = _try_lev
                    raw_lot         = _try_lot
                    target_notional = _try_notional
                    print(f"  ⚡ [{symbol}] leverage eskalasi → {trade_leverage}x | lot={raw_lot:.4f} | notional={target_notional:.2f}")
                    break
            else:
                # Semua leverage sudah dicoba, masih tidak bisa penuhi minNotional
                # tanpa melanggar SL cap → skip
                _lot_min_str = f"{_lot_for_notional:.4f}"
                send_telegram_raw(
                    f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                    f"Notional minimum {min_notional:.0f} USDT butuh lot {_lot_min_str}, "
                    f"tapi loss jika SL kena <b>{_loss_for_notional:.2f} USDT</b> melebihi batas "
                    f"{max_loss_usdt:.2f} USDT ({MAX_SL_LOSS_PCT*100:.1f}% balance).\n"
                    f"Naikkan /setmarginratio atau perbesar balance."
                )
                record_pair_fail(symbol)
                return False
        else:
            # FIXED_LEVERAGE aktif, tidak bisa eskalasi → skip
            send_telegram_raw(
                f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Notional minimum {min_notional:.0f} USDT butuh lot lebih besar dari batas SL. "
                f"Loss jika SL kena: <b>{_loss_for_notional:.2f} USDT</b> > {max_loss_usdt:.2f} USDT."
            )
            record_pair_fail(symbol)
            return False

    # ── Round lot ke stepSize ─────────────────────────────────────────────────
    lot = round_lot_to_step(raw_lot, step_size)
    if qty_prec is not None:
        lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)

    # ── Handle minNotional setelah rounding (lot bisa turun karena floor) ────
    notional = lot * _entry
    if notional < min_notional:
        # Rounding turunkan lot → notional jadi kurang → naik 1 step
        lot_needed = round_lot_up_to_step(
            math.ceil(min_notional / max(_entry, 1e-9) / step_size) * step_size, step_size
        )
        if qty_prec is not None:
            lot_needed = round(math.ceil(lot_needed * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        _loss_needed = lot_needed * eff_sl_dist
        if _loss_needed > max_loss_usdt * 1.05:   # toleransi 5% overshoot akibat rounding
            send_telegram_raw(
                f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Notional minimum {min_notional:.0f} USDT butuh lot {lot_needed}, "
                f"tapi loss jika SL kena <b>{_loss_needed:.2f} USDT</b> melebihi batas "
                f"{max_loss_usdt:.2f} USDT ({MAX_SL_LOSS_PCT*100:.1f}% balance).\n"
                f"Naikkan /setmarginratio atau perbesar balance."
            )
            record_pair_fail(symbol)
            return False
        lot      = lot_needed
        notional = lot * _entry
        print(f"  ℹ️  [{symbol}] lot rounding-up ke {lot} agar penuhi minNotional={min_notional:.2f}")

    # ── Round ke mkt_stepSize dan enforce quantityPrecision ──────────────────
    lot = round_lot_to_step(lot, mkt_step_size)
    if qty_prec is not None:
        lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
    notional = lot * _entry

    # ── Pastikan tetap memenuhi minQty/minNotional (naikkan, JANGAN skip) ────
    # mkt_stepSize bisa lebih kasar dari step_size dan menurunkan lot lagi di
    # bawah minQty/minNotional setelah floor di atas. Naikkan lot seminimal
    # mungkin agar tetap memenuhi kedua batas ini — jangan langsung skip hanya
    # karena minQty/minNotional. Risk hasil kenaikan ini akan dicek ulang di
    # GUARD FINAL (actual_loss vs max_loss_usdt) setelah blok margin di bawah.
    if lot < min_qty or notional < min_notional:
        _lot_for_min = max(min_qty, min_notional / max(_entry, 1e-9))
        lot = round_lot_up_to_step(_lot_for_min, mkt_step_size)
        if qty_prec is not None:
            lot = round(math.ceil(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        notional = lot * _entry
        print(f"  ℹ️  [{symbol}] lot dinaikkan ulang ke {lot} (mkt_stepSize rounding) agar tetap penuhi minQty/minNotional")

    # ── Validasi final: skip HANYA jika BENAR2 masih tidak valid setelah dinaikkan ──
    # (kasus ekstrem: minQty/minNotional exchange tidak bisa dicapai sama sekali
    # lewat rounding di atas — sangat jarang, tapi tetap dijaga sebagai fallback).
    if lot < min_qty or notional < min_notional:
        _msg = (f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Setelah rounding & penyesuaian: lot={lot} notional={notional:.2f} masih tidak "
                f"memenuhi minQty={min_qty}/minNotional={min_notional} — batas exchange.")
        print(f"⛔ [{symbol}] lot/notional tidak valid setelah rounding & adjustment, skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return False

    # ── Re-pick leverage dari bracket Binance setelah notional final ────────
    # Setelah lot final diketahui, cari leverage MAKSIMUM yang diizinkan Binance
    # untuk notional tersebut. Ini meminimalkan margin_required = notional/leverage.
    # Tanpa ini: BTCUSDT hardcode 20x → margin $137; dengan ini bisa 125x → margin $22.
    # Per-symbol override (PER_SYMBOL_LEVERAGE) tetap dihormati sebagai BATAS BAWAH,
    # bukan batas atas — kita pakai maksimum dari keduanya.
    notional = lot * _entry
    _bracket_lev = get_max_leverage_for_notional(symbol, notional)
    if symbol in PER_SYMBOL_LEVERAGE:
        # Ambil yang LEBIH TINGGI agar margin makin kecil
        trade_leverage = max(_bracket_lev, PER_SYMBOL_LEVERAGE[symbol])
    elif FIXED_LEVERAGE > 0:
        trade_leverage = FIXED_LEVERAGE  # fixed override tetap dihormati
    else:
        trade_leverage = _bracket_lev
    print(f"  ⚡ [{symbol}] Leverage final: {trade_leverage}x (bracket max={_bracket_lev}x) | notional={notional:.2f}")

    # ── Cek margin: kalau tidak cukup, perkecil lot dulu sebelum skip ────────
    # _pending_margin_used dan _effective_available sudah dihitung di atas (sebelum sizing).
    # Tidak perlu hitung ulang — langsung pakai nilai yang sudah ada.

    _required_margin   = notional / max(trade_leverage, 1)
    _max_margin_ok     = _effective_available * MARGIN_SAFETY_FACTOR
    _lot_shrunk_margin = False
    if _required_margin > _max_margin_ok:
        _max_notional_fit = _effective_available * MARGIN_SAFETY_FACTOR * trade_leverage
        _lot_fit = round_lot_to_step(_max_notional_fit / max(_entry, 1e-9), step_size)
        if qty_prec is not None:
            _lot_fit = round(math.floor(_lot_fit * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        if _lot_fit < min_qty or (_lot_fit * _entry) < min_notional:
            _msg = (
                f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Margin tidak cukup bahkan di leverage {trade_leverage}x untuk lot minimum exchange.\n"
                f"Available: {available_bal:.2f} USDT (efektif: {_effective_available:.2f}) | Margin dibutuhkan: {_required_margin:.2f} USDT"
            )
            print(f"  ⛔ [{symbol}] margin tidak cukup untuk min lot: avail={_effective_available:.2f} req={_required_margin:.2f}")
            send_telegram_raw(_msg)
            record_pair_fail(symbol)
            return False
        print(f"  ⚠️  [{symbol}] Lot diperkecil {lot:.6f} → {_lot_fit:.6f} demi fit margin "
              f"(efektif {_effective_available:.2f} USDT @ {trade_leverage}x)")
        lot               = _lot_fit
        notional          = lot * _entry
        # Re-check bracket setelah lot turun (bisa dapat leverage lebih tinggi)
        if FIXED_LEVERAGE <= 0:
            trade_leverage = get_leverage_for_price(_entry, symbol=symbol)
        _required_margin   = notional / max(trade_leverage, 1)
        _lot_shrunk_margin = True

    # ── GUARD FINAL: loss aktual tidak boleh melebihi MAX_SL_LOSS_PCT efektif ──
    actual_loss = lot * eff_sl_dist
    if actual_loss > max_loss_usdt * 1.001:
        _msg = (f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Lot minimum exchange ({lot}) menghasilkan potensi loss "
                f"<b>{actual_loss:.2f} USDT</b> jika SL kena — melebihi batas "
                f"{MAX_SL_LOSS_PCT*100:.2f}% ({max_loss_usdt:.2f} USDT) dari balance"
                + ".\nNaikkan /setmarginratio atau perbesar balance.")
        print(f"⛔ [{symbol}] actual_loss={actual_loss:.2f} > max_loss={max_loss_usdt:.2f}, skip")
        send_telegram_raw(_msg)
        record_pair_fail(symbol)
        return False

    # ── FLOOR REALISASI LOSS: jangan biarkan lot rounding/minNotional/margin-fit
    # menjatuhkan realisasi terlalu jauh DI BAWAH target /setmarginratio ──────
    # Kasus nyata yang dilaporkan user: target 1% dari $5000 = $50, tapi
    # realisasi kadang cuma $45, $49, bahkan sampai $15 — akibat floor lot ke
    # bawah step-size (round_lot_to_step) dan rounding minNotional yang semua
    # mengarah ke bawah, terakumulasi. Sekarang: kalau actual_loss < 75% dari
    # max_loss_usdt, coba naikkan lot 1 step demi 1 step (maksimal beberapa
    # kali) sampai actual_loss balik ke floor 75% — TAPI tidak pernah melebihi
    # max_loss_usdt sendiri (guard di atas) dan tidak pernah melebihi margin
    # yang tersedia. Kalau exchange step-size-nya kasar sekali sehingga tidak
    # mungkin capai floor 75% tanpa overshoot max_loss_usdt, lot dibiarkan apa
    # adanya (lebih aman under-risk daripada over-risk).
    REALIZED_LOSS_FLOOR_PCT = 0.75   # realisasi minimal 75% dari target /setmarginratio
    _floor_loss_usdt = max_loss_usdt * REALIZED_LOSS_FLOOR_PCT
    if actual_loss < _floor_loss_usdt:
        _orig_lot, _orig_loss = lot, actual_loss
        _step_for_bump = mkt_step_size if mkt_step_size > 0 else step_size
        _max_bump_notional = _effective_available * MARGIN_SAFETY_FACTOR * trade_leverage
        _bump_tries = 0
        while actual_loss < _floor_loss_usdt and _bump_tries < 200:
            _try_lot = lot + _step_for_bump
            if qty_prec is not None:
                _try_lot = round(_try_lot, qty_prec)
            _try_loss     = _try_lot * eff_sl_dist
            _try_notional = _try_lot * _entry
            if _try_loss > max_loss_usdt * 1.001 or _try_notional > _max_bump_notional:
                break   # naik 1 step lagi bakal overshoot max_loss atau margin — berhenti
            lot          = _try_lot
            actual_loss  = _try_loss
            notional     = _try_notional
            _bump_tries += 1
        if lot != _orig_lot:
            print(f"  📈 [{symbol}] Lot dinaikkan {_orig_lot} → {lot} agar realisasi loss "
                  f"{_orig_loss:.2f} → {actual_loss:.2f} USDT (floor {REALIZED_LOSS_FLOOR_PCT*100:.0f}% "
                  f"dari target {max_loss_usdt:.2f})")
        else:
            print(f"  ℹ️  [{symbol}] Realisasi loss {actual_loss:.2f} < floor {_floor_loss_usdt:.2f} "
                  f"tapi tidak bisa dinaikkan tanpa overshoot max_loss/margin — dibiarkan apa adanya")

    margin_usdt = notional / max(trade_leverage, 1)
    _shrink_note = " ⚠️ (lot diperkecil — margin terbatas)" if _lot_shrunk_margin else ""
    print(f"  📐 [{symbol}] lot={lot} notional={notional:.2f} margin={margin_usdt:.2f} "
          f"lev={trade_leverage}x loss_if_SL={actual_loss:.2f}/{max_loss_usdt:.2f} USDT{_shrink_note}")

    market_price = get_current_price(symbol)
    if market_price is None:
        _msg = f"⛔ <b>SKIP ENTRY — {symbol}</b>\nGagal ambil harga market. Cek koneksi API Binance."
        print(f"⛔ Gagal ambil harga market {symbol}, skip")
        send_telegram_raw(_msg)
        return False

    # Validasi pair bisa di-trade (sudah gagal ≥3x berturut → cooldown sementara)
    if not validate_pair_tradeable(symbol, market_price):
        print(f"⛔ [{symbol}] di-skip sementara (cooldown gagal berturut) — tidak kirim notif")
        return   # silent skip — sudah ada notif dari record_pair_fail sebelumnya

    # ── Gunakan presisi natural harga, bukan tickSize dari PRICE_FILTER ──────
    # Binance algo/stop orders jauh lebih fleksibel soal presisi dibanding
    # limit orders. tickSize dari PRICE_FILTER (misal 0.1 atau 0.01) terlalu
    # kasar untuk harga seperti ADA 0.2437 — akan membulatkan jadi 0.2 atau 0.24.
    # Solusi: hitung presisi dari harga market itu sendiri (4-6 sig. digit).
    def smart_price_precision(price: float) -> int:
        """Hitung desimal presisi yang tepat berdasarkan harga."""
        if price <= 0:
            return 8
        if price >= 1000:    return 2
        if price >= 100:     return 2
        if price >= 10:      return 3
        if price >= 1:       return 4
        if price >= 0.1:     return 4
        if price >= 0.01:    return 5
        if price >= 0.001:   return 6
        if price >= 0.0001:  return 7
        return 8  # nano price: 1000SHIB/PEPE (~0.00001x)

    price_prec = smart_price_precision(market_price)
    # Pastikan tidak lebih kasar dari tickSize asli exchange
    tick_prec  = max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4
    eff_prec   = max(price_prec, tick_prec)

    def rnd(p): return round_price_to_tick(p, tick_size) if tick_size > 0 else round(p, eff_prec)

    entry = rnd(signal["entry"])
    sl    = rnd(signal["stop_loss"])
    tp1   = rnd(signal["take_profit"][0])
    tp2   = rnd(signal["take_profit"][1])

    # ── FIX: simpan entry & jarak SL/TP ORIGINAL sebelum clamp apapun ────────
    # ROOT CAUSE bug "entry=SL=TP1=TP2 collapse jadi sama": clamp -4016 dan
    # clamp PERCENT_PRICE dulu masing-masing recalc SL/TP dari `entry` versi
    # SAAT ITU (yang mungkin sudah hasil clamp sebelumnya). Kalau dua clamp
    # jalan berurutan dan saling tarik ke arah berlawanan (mis. -4016 geser
    # entry turun, lalu PERCENT_PRICE geser naik lagi), jarak SL/TP yang
    # dipakai untuk recalc kedua sudah korup dari recalc pertama → SL/TP bisa
    # kolaps mendekati entry, lalu ke-skip oleh guard SL-terlalu-jauh.
    # FIX: SEMUA recalc SL/TP di bawah (baik -4016 maupun PERCENT_PRICE)
    # sekarang selalu pakai jarak dari entry ORIGINAL ini, bukan dari entry
    # hasil clamp sebelumnya — supaya rantai clamp tidak saling merusak.
    _entry_original = entry
    _sl_dist_orig  = abs(_entry_original - sl)
    _tp1_dist_orig = abs(_entry_original - tp1)
    _tp2_dist_orig = abs(_entry_original - tp2)

    # ── Update _limit_price_str setelah entry di-snap ke tickSize ────────────
    # KRITIS: harga limit HARUS kelipatan exact tickSize (Binance -4014).
    # rnd() sekarang menggunakan round_price_to_tick yang snap pakai Decimal.
    # ── Clamp entry ke minPrice filter (cegah -4001 Price less than 0) ─────────
    # Binance reject order jika price < minPrice dari PRICE_FILTER.
    # Hanya berlaku untuk pair nano-price (1000SHIB, PEPE, GALA, dll) di mana
    # zona demand/supply bisa ada di bawah minPrice exchange.
    # GUARD: jika minPrice > market_price * 0.5, data minPrice kemungkinan
    # corrupt/cross-contaminated (misal minPrice BTCUSDT=261.1 ter-apply ke SOLUSDT)
    # → skip clamp agar entry pair normal tidak di-override ke harga tidak wajar.
    # TODO (root cause, bukan di sini): guard ini sudah menyelamatkan ATOMUSDT
    # dari salah clamp (log 2026-08-16 09:42:42 — minPrice 261.1 ter-baca utk
    # ATOM, padahal harga ATOM ~1.5). Tapi sumber datanya sendiri masih bug —
    # cek fungsi/tempat `filters` dict di-build & di-cache per simbol, kemungkinan
    # ada key collision atau cache yang tidak di-reset antar simbol saat looping.
    _min_price_filter = filters.get("minPrice", 0.0)
    _market_ref = market_price if (market_price and market_price > 0) else entry
    _min_price_sane = _min_price_filter > 0 and _min_price_filter < (_market_ref * 0.5)
    if _min_price_sane and entry < _min_price_filter:
        print(f"  ⚠️  [{symbol}] Entry {entry} < minPrice {_min_price_filter} — clamp ke minPrice")
        entry = round_price_to_tick(_min_price_filter, tick_size)
        # FIX: clamp -4016/PERCENT_PRICE di bawah SELALU recalc SL/TP dari
        # _sl_dist_orig/_tp_dist_orig supaya RR tetap konsisten — tapi clamp
        # minPrice ini TIDAK PERNAH melakukan itu, padahal dia juga mengubah
        # `entry`. Kalau clamp ini kena (entry digeser naik ke minPrice), SL
        # dan TP yang masih dihitung dari entry LAMA jadi salah jarak relatif
        # terhadap entry BARU — bisa memperkecil jarak TP atau memperbesar
        # jarak SL tanpa disadari, RR jadi menyimpang dari yang lolos grading.
        # Sekarang recalc pakai jarak original, sama seperti clamp lain.
        if _entry_original > 0:
            if direction == "LONG":
                sl  = rnd(entry - _sl_dist_orig)
                tp1 = rnd(entry + _tp1_dist_orig)
                tp2 = rnd(entry + _tp2_dist_orig)
            else:
                sl  = rnd(entry + _sl_dist_orig)
                tp1 = rnd(entry - _tp1_dist_orig)
                tp2 = rnd(entry - _tp2_dist_orig)
            print(f"  🔄 [{symbol}] SL/TP recalc setelah minPrice clamp → entry={entry} SL={sl} TP1={tp1} TP2={tp2}")
    elif _min_price_filter > 0 and not _min_price_sane and entry < _min_price_filter:
        print(f"  ⚠️  [{symbol}] minPrice {_min_price_filter} tidak wajar vs market {_market_ref:.4f} — skip clamp")

    # ── FIX -4016: Clamp limit price agar tidak melewati batas Binance ────────
    # Error -4016 terjadi ketika:
    #   LONG  → limit price > market price (buy limit di atas market = instantly fillable
    #            tapi Binance Futures reject ini untuk LIMIT order zona)
    #   SHORT → limit price > market price (sell limit di ATAS market = instantly fillable,
    #            Binance reject ini untuk LIMIT order zona)
    # CATATAN PENTING: SHORT dengan entry < market adalah NORMAL (zona limit SHORT di bawah
    # harga saat ini = valid pending order). JANGAN clamp kondisi ini — itu yang menyebabkan
    # entry hancur jadi 0.1 untuk ADAUSDT (tick_size=0.1, market=0.1438 → clamp ke 0.1).
    # Clamp hanya jika entry melewati market ke arah yang SALAH (LONG di atas, SHORT di atas).
    if market_price and market_price > 0:
        if direction == "LONG" and entry > market_price:
            # FIX: pakai floor (bukan round nearest) — round nearest bisa
            # membulatkan clamp result ke ATAS market_price lagi (no-op),
            # yang tidak menyelesaikan -4016 sama sekali (lihat WIFUSDT case).
            _clamped = floor_price_to_tick(market_price, tick_size)
            print(f"  ⚠️  [{symbol}] -4016 prevention: LONG entry {entry} > market {market_price} — clamp ke {_clamped}")
            entry = _clamped
            # FIX: recalc SL/TP dari jarak ORIGINAL (bukan dari entry sebelum
            # clamp ini), supaya kalau ada clamp lanjutan (PERCENT_PRICE) yang
            # geser entry lagi, jarak yang dipakai tetap konsisten dan tidak
            # terakumulasi jadi rusak.
            if _entry_original > 0:
                sl  = rnd(entry - _sl_dist_orig)
                tp1 = rnd(entry + _tp1_dist_orig)
                tp2 = rnd(entry + _tp2_dist_orig)
                print(f"  🔄 [{symbol}] SL/TP recalc setelah -4016 clamp → SL={sl} TP1={tp1} TP2={tp2}")
        elif direction == "SHORT" and entry > market_price:
            # SHORT entry di ATAS market = akan instantly fillable → Binance reject (-4016)
            # Clamp ke market price agar order diterima sebagai pending limit
            # FIX: floor, bukan nearest — sama alasan seperti LONG di atas.
            _clamped = floor_price_to_tick(market_price, tick_size)
            print(f"  ⚠️  [{symbol}] -4016 prevention: SHORT entry {entry} > market {market_price} (too high) — clamp ke {_clamped}")
            entry = _clamped
            # FIX: recalc dari jarak ORIGINAL, sama seperti kasus LONG di atas.
            if _entry_original > 0:
                sl  = rnd(entry + _sl_dist_orig)
                tp1 = rnd(entry - _tp1_dist_orig)
                tp2 = rnd(entry - _tp2_dist_orig)
                print(f"  🔄 [{symbol}] SL/TP recalc setelah -4016 clamp → SL={sl} TP1={tp1} TP2={tp2}")

    _limit_price     = entry
    _limit_price_str = format_price(_limit_price, tick_size)

    # ── Clamp harga ke PERCENT_PRICE filter (cegah -4016 dan -4024) ──────────
    # Binance Futures punya filter PERCENT_PRICE: harga LIMIT order harus dalam range:
    #   minAllowed = mark_price × multiplierDown  (misal 0.95 = max 5% di bawah mark)
    #   maxAllowed = mark_price × multiplierUp    (misal 1.05 = max 5% di atas mark)
    # Entry zona yang valid secara strategy bisa jatuh di luar range ini kalau
    # zona jauh dari mark price saat ini → clamp ke batas yang diizinkan.
    _mult_up   = filters.get("multiplierUp",   1.05)
    _mult_down = filters.get("multiplierDown", 0.95)
    _pct_band_impossible = False
    if market_price and market_price > 0:
        # FIX ROOT CAUSE -4016 berulang (WIFUSDT dkk): round_price_to_tick pakai
        # ROUND_HALF_UP. Untuk _pct_max (batas ATAS dari Binance), pembulatan
        # nearest bisa menghasilkan angka LEBIH BESAR dari batas asli Binance
        # (mis. maxAllowed asli 0.1816706, tick=0.1 → round nearest jadi 0.2,
        # padahal 0.2 > 0.1816706 → order tetap ditolak walau "sudah di-clamp").
        # Sebaliknya _pct_min (batas BAWAH) harus ceil, bukan floor, agar tidak
        # jatuh di bawah batas asli (-4024).
        _pct_min = ceil_price_to_tick(market_price * _mult_down, tick_size)
        _pct_max = floor_price_to_tick(market_price * _mult_up,   tick_size)
        # Kalau satu tick step saja sudah lebih lebar dari band yang diizinkan
        # Binance (_pct_min > _pct_max), tidak ada harga LIMIT valid yang bisa
        # memuaskan tickSize DAN PERCENT_PRICE sekaligus untuk symbol ini saat
        # ini — tandai supaya caller bisa fallback ke MARKET order.
        _pct_band_impossible = _pct_min > _pct_max
        if _limit_price < _pct_min:
            print(f"  ⚠️  [{symbol}] PERCENT_PRICE clamp: {_limit_price} < minAllowed {_pct_min} (mark={market_price} × {_mult_down}) — clamp naik")
            _limit_price = _pct_min
            _limit_price_str = format_price(_limit_price, tick_size)
            # FIX #2 (bug dari fix #1): kondisi lama `_entry_original != entry`
            # SALAH — kalau entry sempat digeser clamp -4016 (mis. 1.5→1.4) lalu
            # PERCENT_PRICE menggeser BALIK ke nilai yang sama dengan entry
            # original (1.4→1.5), maka _entry_original(1.5) == entry_baru(1.5)
            # → recalc di-SKIP walau SL/TP masih nyangkut di posisi tengah (1.4)
            # yang sudah tidak match sama sekali dengan entry final. Recalc HARUS
            # selalu jalan tiap kali _limit_price berubah oleh clamp ini, apapun
            # nilai entry sebelumnya — bukan hanya kalau beda dari entry ORIGINAL.
            entry = _limit_price
            if _entry_original > 0:
                if direction == "LONG":
                    sl  = rnd(entry - _sl_dist_orig)
                    tp1 = rnd(entry + _tp1_dist_orig)
                    tp2 = rnd(entry + _tp2_dist_orig)
                else:
                    sl  = rnd(entry + _sl_dist_orig)
                    tp1 = rnd(entry - _tp1_dist_orig)
                    tp2 = rnd(entry - _tp2_dist_orig)
                print(f"  🔄 [{symbol}] SL/TP recalc setelah PERCENT_PRICE clamp → entry={entry} SL={sl} TP1={tp1} TP2={tp2}")
        elif _limit_price > _pct_max:
            print(f"  ⚠️  [{symbol}] PERCENT_PRICE clamp: {_limit_price} > maxAllowed {_pct_max} (mark={market_price} × {_mult_up}) — clamp turun")
            _limit_price = _pct_max
            _limit_price_str = format_price(_limit_price, tick_size)
            # FIX #2: sama, hapus syarat `_entry_original != entry` yang salah.
            entry = _limit_price
            if _entry_original > 0:
                if direction == "LONG":
                    sl  = rnd(entry - _sl_dist_orig)
                    tp1 = rnd(entry + _tp1_dist_orig)
                    tp2 = rnd(entry + _tp2_dist_orig)
                else:
                    sl  = rnd(entry + _sl_dist_orig)
                    tp1 = rnd(entry - _tp1_dist_orig)
                    tp2 = rnd(entry - _tp2_dist_orig)
                print(f"  🔄 [{symbol}] SL/TP recalc setelah PERCENT_PRICE clamp → entry={entry} SL={sl} TP1={tp1} TP2={tp2}")

    # ── Guard: harga hasil format tidak boleh <= 0 (cegah -4001) ─────────────
    # Terjadi pada pair nano-price (1000SHIB, 1000PEPE) jika floating-point
    # error saat snap ke tickSize menghasilkan 0 atau negatif.
    try:
        _limit_price_check = float(_limit_price_str)
    except Exception:
        _limit_price_check = 0.0
    if _limit_price_check <= 0:
        # Fallback: format langsung dari signal entry tanpa snap
        _raw_entry = signal["entry"]
        _fb_prec   = max(8, max(0, round(-math.log10(tick_size))) if tick_size > 0 else 8)
        _limit_price_str = f"{_raw_entry:.{_fb_prec}f}"
        print(f"  ⚠️  [{symbol}] Harga snap ke 0 — fallback ke raw entry: {_limit_price_str}")
        _limit_price = _raw_entry
        # Jika masih 0 setelah fallback → skip entry
        if float(_limit_price_str) <= 0:
            send_telegram_raw(f"⛔ <b>SKIP ENTRY — {symbol}</b>\nHarga tidak valid setelah format: {_limit_price_str}")
            return False

    # Debug: print nilai SL/TP vs entry & market sebelum validasi
    print(f"  🔍 SL/TP CHECK {symbol} [{direction}] entry={entry} sl={sl} tp1={tp1} tp2={tp2} market={market_price}")

    # ── Validasi jarak SL minimum (cegah SL terlalu tipis kena market noise) ────
    # FIX SL DIPAKSA CAP FLAT: sama seperti di generator sinyal — kalau sampai
    # ke sini SL masih/kembali terlalu rapat (mis. akibat clamp -4016/tick
    # rounding di atas mengecilkan jarak lagi setelah lolos dari fix pertama),
    # lebarkan pakai ATR (representasi volatilitas asli pair), bukan persentase
    # flat — supaya SL tetap dapat ruang dari noise/liquidity-grab yang
    # proporsional ke pair itu, bukan comply cap doang.
    # NOTE: execute_trade() tidak menerima ATR dari signal generation (signal
    # dict tidak membawa field ini), jadi dihitung ulang di sini dari data 4H
    # terbaru. Kalau fetch gagal, fallback ke floor persentase lama (aman,
    # tidak block entry hanya karena hiccup API).
    _sl_dist_pct = abs(entry - sl) / max(entry, 1e-9)
    if EXEC_SL_WIDEN_ENABLED and _sl_dist_pct < MIN_SL_DISTANCE_PCT:
        _atr_for_widen = 0.0
        try:
            # FIX BUG LAMA: PRE_ZONE_ALERT_TF tidak pernah didefinisikan di
            # manapun di file ini (NameError selalu terjadi, fallback ke floor
            # persentase setiap kali blok ini jalan). Ganti ke "15m" — sama
            # dengan entry_tf yang dipakai konsisten di seluruh sistem.
            _df_atr_chk = fetch_ohlcv_realdata(symbol, "15m", limit=60)
            if _df_atr_chk is not None and len(_df_atr_chk) >= 20:
                _atr_for_widen = calculate_atr(_df_atr_chk) or 0.0
        except Exception as _atr_e:
            print(f"  ⚠️  [{symbol}] Gagal hitung ATR untuk pelebaran SL, fallback ke floor persentase: {_atr_e}")
        _min_dist_price_exec = entry * MIN_SL_DISTANCE_PCT
        _atr_buffer_exec     = _atr_for_widen * SL_ATR_WIDEN_MULT
        _widened_dist_exec   = max(_min_dist_price_exec, _atr_buffer_exec)
        # SL terlalu dekat — perlebar secara otomatis (struktur-aware via ATR)
        if direction == "LONG":
            sl = rnd(entry - _widened_dist_exec)
            # Jika setelah snap ke tickSize SL masih >= entry (tick terlalu kasar),
            # paksa turun minimal 1 tickSize dari entry
            if sl >= entry:
                sl = round_price_to_tick(entry - tick_size, tick_size)
        else:
            sl = rnd(entry + _widened_dist_exec)
            # Jika setelah snap ke tickSize SL masih <= entry,
            # paksa naik minimal 1 tickSize dari entry
            if sl <= entry:
                sl = round_price_to_tick(entry + tick_size, tick_size)
        print(f"  ⚠️  [{symbol}] SL terlalu dekat ({_sl_dist_pct*100:.3f}% < {MIN_SL_DISTANCE_PCT*100:.1f}%) — diperlebar ke {sl}")
        # ── Recalc TP agar RR tetap valid setelah SL diperlebar ───────────────
        _new_sl_dist = abs(entry - sl)
        if direction == "LONG":
            if tp1 <= entry + _new_sl_dist * RR_GRADE_B:
                tp1 = rnd(entry + _new_sl_dist * RR_GRADE_B)
            if tp2 <= tp1:
                tp2 = rnd(entry + _new_sl_dist * (RR_GRADE_A + 0.5))
        else:
            if tp1 >= entry - _new_sl_dist * RR_GRADE_B:
                tp1 = rnd(entry - _new_sl_dist * RR_GRADE_B)
            if tp2 >= tp1:
                tp2 = rnd(entry - _new_sl_dist * (RR_GRADE_A + 0.5))

    # ── Validasi jarak SL maksimum — cegah SL absurd lolos ke order real ────
    # Cap normal per-mode sudah diterapkan saat SL dibentuk (SL_TP_CAPS di
    # signal generation), tapi ini jadi lapisan kedua: kalau entah kenapa SL
    # yang nyampe sini masih jauh melebihi cap (mis. re-clamp/-4016 recalc
    # rantai panjang menggeser distance), skip entry sekalian daripada lolos
    # dengan risk yang gak wajar (pernah kejadian SL 98% dari entry).
    _sl_cap_mode = SL_TP_CAPS.get(mode.get("label") if isinstance(mode, dict) else None,
                                   (MAX_SL_DISTANCE_PCT, 0.09))[0]
    _sl_dist_pct_final = abs(entry - sl) / max(entry, 1e-9)
    if _sl_dist_pct_final > _sl_cap_mode * 3:
        print(f"⛔ [{symbol}] SL {_sl_dist_pct_final*100:.1f}% dari entry — jauh melebihi cap mode "
              f"({_sl_cap_mode*100:.1f}%×3), skip entry demi keamanan")
        # NOTE: sengaja TIDAK kirim ke Telegram — kasus ini terlalu sering terjadi
        # (root cause masih diinvestigasi, kemungkinan tick_size/rounding salah untuk
        # sejumlah pair) sehingga jadi spam notif. Tetap ke-print di log/console
        # supaya masih bisa dipantau & didiagnosis tanpa ganggu Telegram.
        return False

    if direction == "LONG":
        if sl >= entry:
            print(f"⛔ SL/TP invalid LONG {symbol}: sl={sl} >= entry={entry}, skip")
            return False
        if tp1 <= entry or tp2 <= entry or tp2 <= tp1:
            min_move = entry * 0.003
            tp1 = rnd(entry + min_move * RR_GRADE_B)
            tp2 = rnd(entry + min_move * (RR_GRADE_A + 0.5))
            if tp1 <= entry or tp2 <= entry or tp2 <= tp1:
                print(f"⛔ SL/TP invalid LONG {symbol}: tp1={tp1} tp2={tp2} vs entry={entry}, skip")
                return False
    else:
        if sl <= entry:
            print(f"⛔ SL/TP invalid SHORT {symbol}: sl={sl} <= entry={entry}, skip")
            return False
        if tp1 >= entry or tp2 >= entry or tp2 >= tp1:
            # Rekalkulasi TP dengan buffer minimal dari entry
            min_move = entry * 0.003  # minimal 0.3% dari entry
            tp1 = rnd(entry - min_move * RR_GRADE_B)
            tp2 = rnd(entry - min_move * (RR_GRADE_A + 0.5))
            if tp1 >= entry or tp2 >= entry or tp2 >= tp1:
                print(f"⛔ SL/TP invalid SHORT {symbol}: tp1={tp1} tp2={tp2} vs entry={entry}, skip")
                return False

    # ── Guard: TP1 tidak boleh sama atau lebih buruk dari SL ────────────────
    # Bug: bisa terjadi saat SL distance sangat kecil dan RR recalc menghasilkan
    # TP1 yang jatuh ke level SL atau bahkan di bawah entry (untuk LONG).
    _sl_dist_fix = abs(entry - sl)
    if direction == "LONG" and tp1 <= sl:
        tp1 = rnd(entry + _sl_dist_fix * RR_GRADE_B)
        tp2 = rnd(entry + _sl_dist_fix * (RR_GRADE_A + 0.5))
        print(f"  ⚠️  [{symbol}] FIX: tp1 was <= sl → recalc tp1={tp1} tp2={tp2}")
    elif direction == "SHORT" and tp1 >= sl:
        tp1 = rnd(entry - _sl_dist_fix * RR_GRADE_B)
        tp2 = rnd(entry - _sl_dist_fix * (RR_GRADE_A + 0.5))
        print(f"  ⚠️  [{symbol}] FIX: tp1 was >= sl → recalc tp1={tp1} tp2={tp2}")

    # Guard final: pastikan TP1 dan TP2 masih valid setelah fix
    if direction == "LONG":
        if tp1 <= entry:
            tp1 = rnd(entry + _sl_dist_fix * RR_GRADE_B)
        if tp2 <= tp1:
            tp2 = rnd(tp1 + _sl_dist_fix * 0.5)
    else:
        if tp1 >= entry:
            tp1 = rnd(entry - _sl_dist_fix * RR_GRADE_B)
        if tp2 >= tp1:
            tp2 = rnd(tp1 - _sl_dist_fix * 0.5)

    # ── GUARD RR FLOOR — tegakkan RR_GRADE_B setelah SEMUA clamp exchange ──────
    # ROOT CAUSE bug "RR jelek lolos ke order real" (contoh nyata: SNXUSDT
    # RR~0.46 padahal calculate_rr() sudah clamp RR>=1.5 saat sinyal dibuat):
    # guard-guard di atas (tp1<=sl, tp1<=entry, dst) cuma jaga ORDER/ARAH TP
    # masih benar — TIDAK ADA satupun yang re-cek MAGNITUDO RR aktual. Kalau
    # rantai clamp (-4016, PERCENT_PRICE, minPrice, pelebaran MIN_SL_DISTANCE)
    # menggeser entry/SL berkali-kali, TP yang "masih valid secara arah" bisa
    # tetap lolos semua guard di atas walau RR akhirnya sudah jauh di bawah
    # RR_GRADE_B — karena tidak ada guard yang benar-benar menghitung rasio.
    # Ini lapisan terakhir: hitung ulang RR final untuk TP1 dan TP2 masing-
    # masing (TETAP DIJAGA TERPISAH untuk skema partial TP1/TP2), dan kalau
    # salah satu di bawah RR_GRADE_B, paksa TP itu maju supaya RR minimal
    # RR_GRADE_B lagi — SL tetap struktural apa adanya (tidak disentuh).
    _final_sl_dist_rr = abs(entry - sl)
    if _final_sl_dist_rr > 0:
        _final_rr1 = abs(tp1 - entry) / _final_sl_dist_rr
        _final_rr2 = abs(tp2 - entry) / _final_sl_dist_rr
        if _final_rr1 < RR_GRADE_B:
            print(f"  ⚠️  [{symbol}] RR1 final {_final_rr1:.2f} < {RR_GRADE_B} setelah clamp exchange — paksa TP1 naik ke RR minimum")
            if direction == "LONG":
                tp1 = rnd(entry + _final_sl_dist_rr * RR_GRADE_B)
            else:
                tp1 = rnd(entry - _final_sl_dist_rr * RR_GRADE_B)
        # PENTING: TP2 floor pakai target RR SWING (RR_GRADE_A + 0.5), BUKAN
        # RR_GRADE_B yang sama dengan TP1 — sebelumnya keduanya dipaksa ke
        # rumus identik saat RR jatuh, sehingga TP1 == TP2 (lihat bug DOGEUSDT/
        # GMXUSDT: SL melebar akibat clamp exchange -> RR1 & RR2 sama-sama
        # jatuh di bawah floor -> keduanya "diselamatkan" ke titik yang sama).
        _rr2_floor = RR_GRADE_A + 0.5
        if _final_rr2 < _rr2_floor:
            print(f"  ⚠️  [{symbol}] RR2 final {_final_rr2:.2f} < {_rr2_floor} (target swing) — paksa TP2 naik ke RR minimum swing")
            if direction == "LONG":
                tp2 = rnd(entry + _final_sl_dist_rr * _rr2_floor)
            else:
                tp2 = rnd(entry - _final_sl_dist_rr * _rr2_floor)
        # tp2 tidak boleh lebih dekat ke entry daripada tp1 (bisa terjadi kalau
        # tp1 di atas naik ke floor sementara tp2 sudah lebih dulu di atasnya)
        if direction == "LONG":
            tp2 = max(tp2, tp1)
        else:
            tp2 = min(tp2, tp1)
        # kalau setelah clamp max/min di atas tp2 masih persis == tp1 (kasus
        # ekstrem: bahkan _rr2_floor pun ketiban clamp exchange di rnd()),
        # dorong tp2 minimal RR_GRADE_B/2 lebih jauh dari tp1 supaya partial
        # TP1/TP2 tetap punya dua target berbeda, bukan single-TP tersamar.
        if tp2 == tp1:
            _extra = _final_sl_dist_rr * (RR_GRADE_B * 0.5)
            tp2 = rnd(tp1 + _extra) if direction == "LONG" else rnd(tp1 - _extra)
        if _final_rr1 < RR_GRADE_B or _final_rr2 < _rr2_floor:
            print(f"  🔄 [{symbol}] TP di-recalc → tp1={tp1} tp2={tp2} (RR1 floor={RR_GRADE_B}, RR2 floor={_rr2_floor})")

    # ── GUARD FINAL v2: re-cek loss aktual SETELAH semua clamp exchange ────────
    # ROOT CAUSE bug "loss aktual > /setmarginratio cap": lot di atas divalidasi
    # terhadap eff_sl_dist SEBELUM entry/SL melewati clamp -4016, PERCENT_PRICE,
    # dan pelebaran MIN_SL_DISTANCE_PCT (line ~10093-10241). Clamp-clamp itu bisa
    # memperlebar jarak SL tanpa lot ikut mengecil, sehingga actual_loss = lot ×
    # sl_dist_final bisa jauh melebihi max_loss_usdt walau lolos GUARD FINAL awal.
    # Guard ini re-hitung dengan entry/sl FINAL, lalu shrink lot (atau skip kalau
    # shrink bikin lot < minQty/minNotional) supaya cap /setmarginratio benar2 ditegakkan.
    _final_sl_dist = abs(entry - sl)
    _final_loss    = lot * _final_sl_dist
    if _final_loss > max_loss_usdt * 1.05:
        _shrunk_lot = round_lot_to_step(max_loss_usdt / max(_final_sl_dist, 1e-9), step_size)
        if qty_prec is not None:
            _shrunk_lot = round(math.floor(_shrunk_lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        if _shrunk_lot >= min_qty and (_shrunk_lot * entry) >= min_notional:
            print(f"  ⚠️  [{symbol}] Lot diperkecil pasca-clamp: {lot} → {_shrunk_lot} "
                  f"(loss {_final_loss:.2f} → {_shrunk_lot * _final_sl_dist:.2f} USDT, cap {max_loss_usdt:.2f})")
            lot      = _shrunk_lot
            notional = lot * entry
        else:
            _msg = (f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                    f"SL melebar setelah clamp exchange (-4016/PERCENT_PRICE/MIN_SL_DISTANCE) → "
                    f"loss aktual <b>{_final_loss:.2f} USDT</b> melebihi batas "
                    f"<b>{max_loss_usdt:.2f} USDT</b>, dan lot minimum exchange tidak muat di batas risk ini.\n"
                    f"Naikkan /setmarginratio atau perbesar balance.")
            print(f"⛔ [{symbol}] final_loss={_final_loss:.2f} > cap={max_loss_usdt:.2f} pasca-clamp (min lot gak muat), skip")
            send_telegram_raw(_msg)
            record_pair_fail(symbol)
            return False
    elif _final_loss < max_loss_usdt * 0.95:
        # FIX KONSISTENSI MARGIN: sebelumnya kalau SL final ternyata lebih RAPAT
        # dari eff_sl_dist yang dipakai saat sizing awal (mis. SL sinyal kena
        # floor MIN_SL_DISTANCE_PCT di titik lain sehingga jaraknya menyempit
        # dari perkiraan semula), lot TIDAK PERNAH dinaikkan lagi — hanya guard
        # shrink di atas yang aktif. Akibatnya loss realized konsisten MEPET DI
        # BAWAH cap (mis. selalu ~0.80% padahal /setmarginratio di-set 1%),
        # bukan variasi acak — user melihat pola "selalu di bawah target,
        # selisihnya besar" persis karena ini. Sekarang: naikkan lot supaya
        # loss balik mendekati max_loss_usdt, tapi tetap ditahan oleh margin
        # yang tersedia dan limit exchange (minQty/minNotional/lot_maxQty) —
        # jadi tidak pernah melanggar proteksi -2019 atau notional cap lain.
        _target_lot = round_lot_to_step(max_loss_usdt / max(_final_sl_dist, 1e-9), step_size)
        if qty_prec is not None:
            _target_lot = round(math.floor(_target_lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        # Jangan sampai naik lot melanggar cap margin/notional yang sudah
        # divalidasi sebelumnya (baris ~10143-10169) — re-terapkan batas yang sama.
        _max_notional_allowed = _effective_available * MARGIN_SAFETY_FACTOR * trade_leverage
        _target_lot = min(_target_lot, _max_notional_allowed / max(entry, 1e-9))
        # Jangan lebih besar dari lot_maxQty exchange
        _lot_max_exch = get_lot_filters(symbol).get("lot_maxQty", 9999999.0)
        _target_lot = min(_target_lot, _lot_max_exch)
        if _target_lot > lot:
            _new_loss = _target_lot * _final_sl_dist
            print(f"  ℹ️  [{symbol}] Lot dinaikkan pasca-clamp: {lot} → {_target_lot} "
                  f"(loss {_final_loss:.2f} → {_new_loss:.2f} USDT, target {max_loss_usdt:.2f})")
            lot      = _target_lot
            notional = lot * entry
            _final_loss = _new_loss



    if SINGLE_TP_MODE:
        tp2 = tp1
    # ── GUARD: harga sudah melewati TARGET TP TERAKHIR → setup tidak valid ──
    # Contoh: LONG entry 0.1, target TP 0.115 — tapi market sekarang sudah 0.12.
    # Setup sudah "kejemput" duluan, risk/reward tidak lagi valid.
    #
    # REVISI: sebelumnya kirim notif Telegram "SINYAL DIBATALKAN" di titik ini,
    # padahal titik ini terjadi SEBELUM order apapun dikirim ke Binance (order
    # LIMIT entry baru dikirim jauh di bawah guard ini) — jadi tidak ada
    # "pembatalan" yang sebenarnya terjadi, tidak ada limit order yang perlu
    # di-cancel. Notif itu memberi kesan salah seolah bot sempat pasang limit
    # order lalu membatalkannya. Sekarang cukup di-log ke console (untuk
    # debugging kamu sendiri lewat Railway logs), tanpa notif Telegram.
    if market_price:
        _tp_target_check = tp2
        _tp_breached = (
            (direction == "LONG"  and market_price >= _tp_target_check) or
            (direction == "SHORT" and market_price <= _tp_target_check)
        )
        if _tp_breached:
            print(
                f"  ⛔ [{symbol}] Skip entry (belum sempat kirim order ke Binance) — "
                f"market {market_price} sudah melewati target TP {tp2} sebelum limit order dipasang"
            )
            return False

    try:
        # ── Bersihkan conditional orders lama sebelum buka posisi baru ──────────
        # Skenario: posisi sebelumnya sudah close (TP/SL kena atau manual) tapi
        # conditional order TP/SL-nya belum ikut terhapus di Binance.
        # Jika langsung buka posisi baru tanpa clean-up → TP/SL lama masih aktif
        # dengan struktur/harga yang sudah tidak valid untuk posisi baru.
        # Solusi: cancel semua open orders symbol ini sebelum apapun.
        try:
            cancel_open_orders(symbol)
            print(f"  🧹 [{symbol}] Pre-entry cleanup: conditional orders lama dibersihkan.")
        except Exception as _pre_cancel_err:
            print(f"  ⚠️  [{symbol}] Pre-entry cleanup gagal (lanjut): {_pre_cancel_err}")

        # ── Auto-set CROSSED margin agar tidak perlu fund isolated wallet per symbol ──
        # -2019 sering terjadi di ISOLATED mode karena wallet per-symbol kosong.
        # CROSSED pakai total balance langsung → tidak ada masalah isolated wallet.
        try:
            api_post("/fapi/v1/marginType", {"symbol": symbol, "marginType": "CROSSED"})
            print(f"  🔧 [{symbol}] Margin type → CROSSED")
        except Exception as _mt_err:
            _mt_str = str(_mt_err)
            if "No need to change" in _mt_str or "-4046" in _mt_str:
                pass  # sudah CROSSED, tidak perlu ubah
            else:
                print(f"  ⚠️  [{symbol}] Set margin type error (lanjut): {_mt_err}")

        # ── Set leverage dan verifikasi via query posisi (cegah testnet silent cap) ──
        # Binance testnet kadang diam-diam membatasi leverage (misal 125x → 20x)
        # tapi return sukses. Akibatnya margin dihitung dari 125x tapi exchange
        # pakai 20x → -2019. Solusi: query /fapi/v2/positionRisk setelah set_leverage
        # untuk tahu leverage aktual yang benar-benar dipakai Binance.
        print(f"  🔧 [{symbol}] set_leverage dipanggil: request={trade_leverage}x")
        _actual_lev = set_leverage(symbol, trade_leverage)
        print(f"  🔧 [{symbol}] set_leverage return: {_actual_lev}x")
        if _actual_lev is None:
            _actual_lev = trade_leverage  # fallback jika set_leverage error

        # Verifikasi leverage aktual via positionRisk
        try:
            _pos_risk = api_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
            print(f"  🔍 [{symbol}] positionRisk raw: {_pos_risk}")
            for _pr in (_pos_risk if isinstance(_pos_risk, list) else []):
                if _pr.get("symbol") == symbol:
                    _verified_lev = int(_pr.get("leverage", _actual_lev))
                    if _verified_lev != _actual_lev:
                        print(f"  ⚠️  [{symbol}] Leverage verify: set={_actual_lev}x tapi exchange pakai {_verified_lev}x (testnet cap)")
                        _actual_lev = _verified_lev
                    break
        except Exception as _pr_err:
            print(f"  ⚠️  [{symbol}] Gagal verify leverage via positionRisk: {_pr_err}")

        if _actual_lev != trade_leverage:
            print(f"  ⚠️  [{symbol}] Leverage disesuaikan: {trade_leverage}x → {_actual_lev}x")
            trade_leverage = _actual_lev

        # Recalculate margin dengan leverage aktual yang sudah diverifikasi
        _required_margin = notional / max(trade_leverage, 1)
        if _required_margin > _effective_available * MARGIN_SAFETY_FACTOR:
            # Perkecil lot agar fit margin dengan leverage aktual
            lot = round_lot_to_step(
                (_effective_available * MARGIN_SAFETY_FACTOR * trade_leverage) / max(_entry, 1e-9),
                step_size
            )
            if qty_prec is not None:
                lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
            notional = lot * _entry
            _required_margin = notional / max(trade_leverage, 1)
            print(f"  ⚠️  [{symbol}] Lot di-adjust ke {lot} (notional={notional:.2f}) setelah leverage koreksi ke {trade_leverage}x")
        print(f"  ✅ [{symbol}] Leverage verified={trade_leverage}x | margin_req={_required_margin:.2f} USDT | avail={_effective_available:.2f} USDT")

        hedge = is_hedge_mode()
        position_side = ("SHORT" if side == "SELL" else "LONG") if hedge else "BOTH"
        # ── Re-fetch quantityPrecision langsung dari exchange sebelum kirim ──────
        # Binance bisa update filter kapan saja — pakai nilai paling fresh.
        # Ini mencegah -1111 akibat qty_prec stale di cache.
        try:
            # FIX: sama seperti bug filter DOGEUSDT — "symbol" param diabaikan
            # Binance, response berisi SEMUA simbol. Filter manual, jangan
            # asal ambil index [0] (bisa jadi simbol lain sama sekali).
            _live_info = api_get("/fapi/v1/exchangeInfo", {"symbol": symbol})
            _live_syms = [s for s in _live_info.get("symbols", []) if s.get("symbol") == symbol]
            if _live_syms:
                _live_qty_prec = _live_syms[0].get("quantityPrecision", None)
                if _live_qty_prec is not None:
                    _live_qty_prec = int(_live_qty_prec)
                    if qty_prec != _live_qty_prec:
                        print(f"  🔄 [{symbol}] quantityPrecision updated: {qty_prec} → {_live_qty_prec}")
                    qty_prec = _live_qty_prec
        except Exception as _qpe:
            print(f"  ⚠️  [{symbol}] gagal re-fetch qty_prec: {_qpe} — pakai cache qty_prec={qty_prec}")

        # Floor lot ulang sesuai presisi paling ketat sebelum format & kirim
        # Hitung presisi dari mkt_step_size terlebih dahulu
        _mkt_step_prec = max(0, round(-math.log10(mkt_step_size))) if mkt_step_size > 0 else 3
        # Ambil presisi paling ketat: min antara step precision dan qty_prec
        _final_prec = _mkt_step_prec if qty_prec is None else min(_mkt_step_prec, int(qty_prec))
        # Floor lot ke presisi final — BUKAN round, floor (Binance strict)
        _factor = 10 ** _final_prec
        lot = math.floor(lot * _factor) / _factor
        lot = round(lot, _final_prec)   # hapus floating-point noise setelah floor

        # ── Kirim LIMIT order (GTC) dengan auto-retry presisi ────────────────────
        #
        # ── Format lot untuk LIMIT order ─────────────────────────────────────
        # LIMIT order divalidasi Binance menggunakan LOT_SIZE.stepSize (bukan mkt_stepSize).
        # lot sudah dihitung dan divalidasi di atas — re-round ke step_size untuk LIMIT.

        # Clamp lot ke lot_maxQty (LOT_SIZE.maxQty) — batas per-order LIMIT Binance.
        # Berbeda dengan mkt_maxQty (MARKET_LOT_SIZE) yang sangat kecil untuk pair
        # seperti CHZUSDT. lot_maxQty adalah batas yang berlaku untuk LIMIT order.
        _lot_max_for_limit = filters.get("lot_maxQty", filters.get("maxQty", 9999999.0))
        if lot > _lot_max_for_limit:
            print(f"  ⚠️  [{symbol}] Lot {lot} melebihi lot_maxQty={_lot_max_for_limit} — clamp")
            lot = round_lot_to_step(_lot_max_for_limit, step_size)
            if qty_prec is not None:
                lot = round(math.floor(lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)

        _clean_lot = round_lot_to_step(lot, step_size)
        if qty_prec is not None:
            _clean_lot = round(math.floor(_clean_lot * (10 ** qty_prec)) / (10 ** qty_prec), qty_prec)
        _clean_notional = _clean_lot * signal["entry"]

        # Validasi lot: skip jika lot < minQty (invalid secara exchange).
        if _clean_lot < min_qty:
            send_telegram_raw(
                f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"Lot {_clean_lot} di bawah minQty {min_qty} setelah format LIMIT."
            )
            record_pair_fail(symbol)
            return False

        # FIX: notional setelah clamp lot_maxQty bisa jauh di bawah minNotional
        # (bukan cuma "sedikit" meleset) — kasus VETUSDT: lot_maxQty=1000 ×
        # harga 0.0045 = notional 4.53, padahal minNotional=50. Itu BUKAN gap
        # kecil yang bisa ketolong pergerakan harga, itu kontradiksi struktural
        # (lot_maxQty pair ini memang tidak cukup untuk capai minNotional di
        # harga sekarang) — order PASTI ditolak Binance (-4164). Sebelumnya
        # bot tetap "coba kirim" dan baru tahu gagal setelah API call sia-sia
        # + notifikasi error membingungkan user. Sekarang: kalau notional <
        # 90% dari minNotional → skip dari awal, jangan kirim order yang
        # sudah pasti gagal. Kalau cuma sedikit di bawah (90-100%), tetap
        # kirim — masih mungkin lolos karena pergerakan harga kecil.
        _notional_floor_tolerance = min_notional * 0.9
        if _clean_notional < _notional_floor_tolerance:
            send_telegram_raw(
                f"⛔ <b>SKIP ENTRY — {symbol}</b>\n"
                f"{'─'*34}\n"
                f"Notional maksimum yang bisa dicapai (lot_maxQty × harga) = "
                f"<b>{_clean_notional:.2f} USDT</b>, jauh di bawah minNotional "
                f"exchange ({min_notional} USDT).\n"
                f"⚠️ lot_maxQty pair ini ({_lot_max_for_limit}) terlalu kecil "
                f"relatif ke harga saat ini — order PASTI ditolak Binance "
                f"(code -4164) kalau tetap dikirim, jadi di-skip dari awal."
            )
            record_pair_fail(symbol)
            return False
        elif _clean_notional < min_notional:
            print(f"  ⚠️  [{symbol}] Notional {_clean_notional:.2f} < minNotional {min_notional} setelah clamp — masih dalam toleransi, tetap coba kirim")

        # Format qty_str dengan presisi exact
        if qty_prec is not None:
            _qty_str = f"{_clean_lot:.{qty_prec}f}"
        else:
            _fallback_prec = _step_precision(step_size) if step_size > 0 else 3
            _qty_str = f"{_clean_lot:.{_fallback_prec}f}"

        order_params = {
            "symbol":      symbol,
            "side":        side,
            "type":        "LIMIT",
            "timeInForce": "GTC",
            "quantity":    _qty_str,
            "price":       _limit_price_str,
        }
        if hedge:
            order_params["positionSide"] = position_side

        # FIX: kalau tickSize symbol ini lebih lebar dari band PERCENT_PRICE
        # (_pct_band_impossible True) — TIDAK ADA harga LIMIT tick-aligned
        # yang bisa lolos filter Binance, apapun cara kita bulatkan. Ini bukan
        # kasus "salah hitung", tapi kontradiksi struktural antara tickSize
        # dan multiplierUp/Down exchange untuk pair ini saat ini. Daripada
        # skip entry (yang user tidak mau — modal $5000 tidak masuk akal ada
        # sinyal valid yang tidak ke-entry), langsung kirim MARKET order agar
        # tetap entry, lalu SL/TP tetap dipasang berbasis harga fill aktual.
        if _pct_band_impossible:
            print(f"  ⚡ [{symbol}] PERCENT_PRICE band lebih sempit dari 1 tick ({tick_size}) — tidak ada harga LIMIT valid. Fallback ke MARKET order agar tetap entry.")
            order_params.pop("price", None)
            order_params.pop("timeInForce", None)
            order_params["type"] = "MARKET"

        print(f"  📤 {order_params['type']} ORDER: {symbol} | side={side} | qty={_qty_str} | price={_limit_price_str if order_params['type']=='LIMIT' else 'MARKET'} | notional={_clean_notional:.2f} | lev={trade_leverage}x")

        # Init _cur_lot dengan nilai awal — akan di-update jika precision retry
        _cur_lot = _clean_lot

        # Kirim — maksimal 2 attempt (attempt 1: normal, attempt 2: floor prec 1 level)
        # -1111 bisa berarti qty ATAU price precision over maximum.
        # Attempt 2: turunkan qty_prec 1 level DAN turunkan price precision 1 level.
        for _attempt in range(2):
            try:
                _o_params = sign_request(dict(order_params))
                _o_r = requests.post(get_base_url() + "/fapi/v1/order", params=_o_params, headers=get_headers())
                if _o_r.ok:
                    _order_resp = _o_r.json()
                    _order_ok   = True
                    break
                _o_err  = {}
                try: _o_err = _o_r.json()
                except Exception: pass
                _o_code = _o_err.get("code", 0)
                _o_msg  = _o_err.get("msg", "?")
                print(f"  ⚠️  LIMIT ORDER attempt {_attempt+1}/2 → HTTP {_o_r.status_code} code={_o_code} msg={_o_msg}")
                if _o_code == -1007:
                    # FIX v64 -1007: "Timeout waiting for response from backend server.
                    # Send status unknown; execution status unknown."
                    # Binance sendiri bilang TIDAK TAHU apakah order tereksekusi atau
                    # tidak. Sebelumnya kode ini jatuh ke "else: raise" — order langsung
                    # dianggap gagal tanpa verifikasi, padahal order BISA JADI sudah
                    # tembus di Binance. Risiko: posisi nyasar tanpa SL/TP terpasang
                    # bot (bot pikir gagal, padahal ada posisi live tanpa proteksi).
                    # FIX: verifikasi langsung ke openOrders sebelum menyerah.
                    time.sleep(1.0)  # beri waktu backend Binance selesai proses
                    try:
                        _verify_ords = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
                        _match = None
                        for _vo in (_verify_ords or []):
                            if (_vo.get("side") == side
                                    and _vo.get("type") == "LIMIT"
                                    and abs(float(_vo.get("origQty", 0)) - float(_qty_str)) < 1e-9):
                                _match = _vo
                                break
                        if _match:
                            # Order TERNYATA berhasil masuk — treat sebagai sukses,
                            # jangan retry (retry lagi bisa jadi DOUBLE ENTRY).
                            _order_resp = _match
                            _order_ok   = True
                            print(f"  ✅ [{symbol}] -1007 tapi order TERVERIFIKASI ada di openOrders (orderId={_match.get('orderId')}) — lanjut sebagai sukses")
                            break
                        else:
                            print(f"  ℹ️  [{symbol}] -1007 dan order TIDAK ditemukan di openOrders — aman untuk retry")
                    except Exception as _verify_err:
                        print(f"  ⚠️  [{symbol}] -1007 verify gagal ({_verify_err}) — treat sebagai gagal, jangan retry (hindari double entry)")
                        raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg} (verifikasi status juga gagal)")
                    if _attempt == 0:
                        time.sleep(0.5)
                        continue  # retry sekali (order sudah confirmed tidak ada)
                    else:
                        raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg} (setelah verifikasi + retry)")
                if _o_code == -4016 and _attempt == 0:
                    # FIX -4016: "Limit price can't be higher than X"
                    # ROOT CAUSE lama: retry ambil fresh market lalu clamp pakai
                    # round_price_to_tick (nearest) — nearest BISA membulatkan
                    # ke ATAS batas asli X (contoh nyata WIFUSDT: X=0.1816706,
                    # tickSize=0.1 → nearest jadi 0.2 > X → order ditolak LAGI
                    # dengan error identik, retry sia-sia). FIX: parse batas X
                    # asli dari pesan error Binance (source of truth, tidak
                    # tergantung staleness get_current_price), lalu floor ke
                    # tick supaya hasil PASTI <= X.
                    import re as _re4016
                    _m4016 = _re4016.search(r'[0-9]+(?:\.[0-9]+)?', _o_msg)
                    _true_max = float(_m4016.group()) if _m4016 else None
                    if _true_max and _true_max > 0:
                        _fixed_price = floor_price_to_tick(_true_max, tick_size)
                        if _fixed_price <= 0 or _fixed_price > _true_max:
                            # Tick lebih lebar dari batas itu sendiri — tidak ada
                            # harga LIMIT valid sama sekali. Fallback MARKET.
                            print(f"  ⚡ [{symbol}] -4016 retry: tickSize={tick_size} lebih lebar dari batas {_true_max} — tidak ada harga LIMIT valid, fallback ke MARKET order")
                            order_params.pop("price", None)
                            order_params.pop("timeInForce", None)
                            order_params["type"] = "MARKET"
                        else:
                            _fixed_price_str = format_price(_fixed_price, tick_size)
                            order_params["price"] = _fixed_price_str
                            _limit_price = _fixed_price
                            print(f"  🔄 [{symbol}] -4016 retry: clamp ke {_fixed_price_str} (floor dari batas asli {_true_max}, bukan market price)")
                        time.sleep(0.2)
                    else:
                        # Tidak bisa parse batas dari pesan — fallback ke fresh
                        # market dengan floor (masih lebih aman dari nearest).
                        _fresh_mkt = get_current_price(symbol)
                        if _fresh_mkt and _fresh_mkt > 0:
                            _fixed_price = floor_price_to_tick(_fresh_mkt, tick_size)
                            _fixed_price_str = format_price(_fixed_price, tick_size)
                            order_params["price"] = _fixed_price_str
                            _limit_price = _fixed_price
                            print(f"  🔄 [{symbol}] -4016 retry: price floor ke market {_fixed_price_str}")
                            time.sleep(0.2)
                        else:
                            raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg}")
                elif _o_code == -1111 and _attempt == 0:
                    # FIX: Precision error — coba turunkan qty precision sampai 0 (integer)
                    # dan turunkan price precision 1 level.
                    # Sebelumnya hanya turun 1 level, yang tidak cukup untuk pair micro-price
                    # (misal 1000SHIBUSDT quantityPrecision=0 tapi masih kirim decimal).
                    _retried = False

                    # Turunkan qty precision — coba floor ke integer (quantityPrecision=0)
                    # Ini fix utama untuk 1000SHIB/PEPE yang qty harus bulat
                    _cur_qty_prec = qty_prec if qty_prec is not None else (
                        max(0, round(-math.log10(step_size))) if step_size > 0 else 3
                    )
                    if _cur_qty_prec > 0:
                        _lower_prec = 0   # FIX: langsung floor ke integer (paling aman)
                        _cur_lot    = math.floor(_clean_lot)
                        _cur_lot    = max(_cur_lot, math.floor(min_qty))
                        _qty_str    = f"{int(_cur_lot)}"
                        order_params["quantity"] = _qty_str
                        print(f"  🔄 [{symbol}] Precision retry qty: {_qty_str} (prec {_cur_qty_prec}→0, floor to int)")
                        _retried = True
                    elif qty_prec == 0:
                        # Sudah di prec=0 tapi masih -1111 → qty sudah integer, masalah di price
                        _cur_lot  = math.floor(_clean_lot)
                        _qty_str  = f"{int(_cur_lot)}"
                        order_params["quantity"] = _qty_str
                        _retried = True

                    # Turunkan price precision 1 level — snap ulang ke tickSize dengan presisi lebih rendah
                    # GUARD: jangan turunkan kalau hasilnya di bawah market price (SHORT akan kena -4024)
                    _cur_tick_prec = max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4
                    if _cur_tick_prec > 0:
                        _lower_price_prec = max(0, _cur_tick_prec - 1)
                        _price_factor = 10 ** _lower_price_prec
                        _lower_price = math.floor(_limit_price * _price_factor) / _price_factor
                        _lower_price = round(_lower_price, _lower_price_prec)
                        # Safety: jika harga turun di bawah 90% market price, terlalu jauh → skip price retry
                        _price_floor = (market_price * 0.90) if (market_price and market_price > 0) else 0
                        if _lower_price < _price_floor:
                            print(f"  ⚠️  [{symbol}] -1111 price retry skip: {_lower_price} < 90% market {_price_floor:.4f} — hanya retry qty")
                        else:
                            _new_price_str = f"{_lower_price:.{_lower_price_prec}f}"
                            order_params["price"] = _new_price_str
                            print(f"  🔄 [{symbol}] Precision retry price: {_new_price_str} (prec {_cur_tick_prec}→{_lower_price_prec})")
                            _retried = True

                    if not _retried:
                        raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg}")
                    time.sleep(0.3)
                elif _o_code == -4014 and _attempt == 0:
                    # FIX -4014: "Price not increased by tick size"
                    # Harga tidak snap ke kelipatan tickSize yang benar.
                    # Kemungkinan tickSize yang dipakai salah (terlalu halus vs PRICE_FILTER).
                    # Solusi: paksa snap ke PRICE_FILTER raw_tick (lebih kasar, pasti valid).
                    _raw_tick_fallback = filters.get("price_filter_tick", tick_size)
                    if _raw_tick_fallback > 0:
                        from decimal import Decimal, ROUND_DOWN
                        _prec_fb = max(0, round(-math.log10(_raw_tick_fallback)))
                        _d_price = Decimal(str(_limit_price))
                        _d_tick_fb = Decimal(str(_raw_tick_fallback))
                        _snapped_fb = (_d_price / _d_tick_fb).to_integral_value(rounding=ROUND_DOWN) * _d_tick_fb
                        _fixed_price_fb = float(round(_snapped_fb, _prec_fb))
                        _fixed_str_fb = f"{_fixed_price_fb:.{_prec_fb}f}"
                        order_params["price"] = _fixed_str_fb
                        _limit_price = _fixed_price_fb
                        print(f"  🔄 [{symbol}] -4014 retry: snap ke PRICE_FILTER tick={_raw_tick_fallback} → {_fixed_str_fb}")
                        time.sleep(0.2)
                    else:
                        raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg}")
                elif _o_code == -4024 and _attempt == 0:
                    # FIX -4024: "Limit price can't be lower than X"
                    # Terjadi ketika limit price terlalu jauh di bawah mark price
                    # (Binance Futures punya batas minimum persentase dari mark price).
                    # Solusi: parse minPrice dari error message lalu clamp ke sana.
                    import re as _re
                    _match = _re.search(r'[0-9]+(?:\.[0-9]+)?', _o_msg)
                    if _match:
                        _min_allowed = float(_match.group())
                        # FIX: ceil (bukan nearest) — sama alasan seperti -4016,
                        # tapi terbalik: hasil clamp harus >= batas asli, nearest
                        # bisa membulatkan ke BAWAH batas dan tetap ditolak.
                        _fixed_price = ceil_price_to_tick(_min_allowed, tick_size)
                        if _fixed_price <= 0:
                            print(f"  ⚡ [{symbol}] -4024 retry: tickSize={tick_size} tidak cocok dengan batas {_min_allowed} — fallback ke MARKET order")
                            order_params.pop("price", None)
                            order_params.pop("timeInForce", None)
                            order_params["type"] = "MARKET"
                        else:
                            _fixed_price_str = format_price(_fixed_price, tick_size)
                            order_params["price"] = _fixed_price_str
                            _limit_price = _fixed_price
                            print(f"  🔄 [{symbol}] -4024 retry: clamp price ke minAllowed {_fixed_price_str} (dari msg: {_o_msg})")
                        time.sleep(0.2)
                    else:
                        raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg}")
                else:
                    # Error bukan precision — jangan coba lagi
                    raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | code={_o_code} msg={_o_msg}")
            except RuntimeError:
                raise
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as _net_err:
                # FIX v64: network-level timeout/connection error saat POST order.
                # Sama seperti -1007 — kita TIDAK TAHU apakah request sempat sampai
                # ke Binance dan tereksekusi sebelum koneksi putus. Verifikasi dulu
                # ke openOrders sebelum menyerah, supaya tidak salah anggap gagal
                # padahal order sudah live tanpa SL/TP ter-track oleh bot.
                print(f"  ⚠️  [{symbol}] Network error saat kirim order: {_net_err} — verifikasi ke openOrders")
                time.sleep(1.0)
                try:
                    _verify_ords2 = api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
                    _match2 = None
                    for _vo2 in (_verify_ords2 or []):
                        if (_vo2.get("side") == side
                                and _vo2.get("type") == "LIMIT"
                                and abs(float(_vo2.get("origQty", 0)) - float(_qty_str)) < 1e-9):
                            _match2 = _vo2
                            break
                    if _match2:
                        _order_resp = _match2
                        _order_ok   = True
                        print(f"  ✅ [{symbol}] Network error tapi order TERVERIFIKASI ada di openOrders (orderId={_match2.get('orderId')}) — lanjut sebagai sukses")
                        break
                    else:
                        print(f"  ℹ️  [{symbol}] Network error dan order TIDAK ditemukan — aman, treat sebagai gagal")
                        raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | network error: {_net_err} (terverifikasi tidak ada order)")
                except RuntimeError:
                    raise
                except Exception as _verify_err2:
                    print(f"  ⚠️  [{symbol}] Verifikasi openOrders juga gagal ({_verify_err2}) — treat sebagai gagal (hindari asumsi sukses tanpa bukti)")
                    raise RuntimeError(f"LIMIT ORDER gagal [{symbol}] | network error: {_net_err} (verifikasi status juga gagal)")
            except Exception:
                raise

        if not _order_ok:
            raise RuntimeError(f"LIMIT ORDER gagal setelah precision + lot-adjust [{symbol}]")


        lot = _cur_lot
        _limit_order_id = _order_resp.get("orderId") if _order_resp else None

        # Reset fail counter
        if symbol in _untradeable_symbols:
            del _untradeable_symbols[symbol]

        # ── TP TUNGGAL ───────────────────────────────────────────────────────
        # Target yang dipakai adalah TP TERAKHIR (tp2 hasil calculate_rr).
        # Tidak ada lagi partial TP1 / TP2 kedua. Seluruh lot ditutup sekaligus
        # ketika harga mencapai target ini. Field tp1/tp2 tetap disimpan sebagai
        # alias kompatibilitas dengan state lama, tetapi nilainya SAMA.
        tp_target = tp2
        tp1_size = round_lot_to_step(lot, mkt_step_size)
        if tp1_size < min_qty:
            tp1_size = round_lot_to_step(min_qty, mkt_step_size)
        tp1_size = min(tp1_size, lot)
        tp2_size = 0.0

        def rtick(p): return round_price_to_tick(p, tick_size)
        sl = rtick(sl)
        tp_target = rtick(tp_target)
        tp1 = tp_target  # compatibility alias
        tp2 = tp_target  # compatibility alias

        # ── Simpan ke pending_limit_orders — SL/TP dipasang SETELAH order terisi ──
        # Monitor loop (monitor_pending_limit_orders) akan cek status setiap scan.
        # Jika terisi → pasang SL/TP dan pindah ke active_positions.
        # Jika timeout LIMIT_ORDER_TIMEOUT_MINUTES menit → cancel order.
        _sig_mode = signal.get("mode", "")
        _entry_type = (
            "🎯 Pre-Zone Sniper" if _sig_mode == "PRE_ZONE"
            else {
                "LOW_TF":   "⚡ Scalping (15m)",
                "HTF_4H":   "🎯 SMC Selective (HTF 4h/Entry 1h)",
                "LTF_30M":  "⚡ Scalping (30m)",
                "SCALPING":  "🔄 Intraday (1h)",
                "INTRADAY":  "📈 Swing (1d→1h)",
                "GOLD_SCALP":"🥇 Gold Scalp",
            }.get(mode.get("label", ""), f"📊 {mode.get('label', '?')}")
        )
        pending_limit_orders[symbol] = {
            "order_id":      _limit_order_id,
            "direction":     direction,
            "entry":         entry,
            "sl":            sl,
            "tp1":           tp1,
            "tp2":           tp2,
            "lot":           lot,
            "tp1_size":      tp1_size,
            "tp2_size":      tp2_size,
            "side":          side,
            "sl_side":       sl_side,
            "position_side": position_side,
            "tick_size":     tick_size,
            "eff_prec":      eff_prec,
            "step_size":     step_size,
            "mkt_step_size": mkt_step_size,
            "min_qty":       min_qty,
            "mode":          mode,
            "signal":        signal,
            "entry_type":    _entry_type,
            "placed_at":     datetime.now(timezone.utc),
        }

        bot_state["signals_today"] += 1

        # ── Notif LIMIT ORDER DIKIRIM — skip untuk Pre-Zone (punya notif sendiri) ──
        _is_pre_zone = signal.get("mode") == "PRE_ZONE"
        if not _is_pre_zone:
            dir_em = "🟢" if direction == "LONG" else "🔴"
            # ── Gunakan eff_prec untuk notif — lebih akurat dari tick_size mentah ──
            # tick_size dari PRICE_FILTER bisa terlalu kasar (misal 0.001 untuk IMXUSDT
            # harga 0.16xx) sehingga entry/SL/TP tampak sama semua di Telegram.
            # eff_prec = max(smart_price_precision(market_price), tick_prec) → presisi benar.
            def _fmt_notif(p: float) -> str:
                return f"{p:.{eff_prec}f}"
            send_telegram_raw(
                f"⏳ <b>LIMIT ORDER DIKIRIM — {symbol}</b>\n"
                f"{'─'*34}\n"
                f"{dir_em} Arah       : <b>{direction}</b>\n"
                f"💲 Entry Limit: <b>{_fmt_notif(entry)}</b>\n"
                f"🛑 Stop Loss  : <b>{_fmt_notif(sl)}</b>\n"
                f"🎯 Target TP  : <b>{_fmt_notif(tp_target)}</b>\n"
                f"📦 Lot        : <b>{lot}</b>\n"
                f"⚡ Leverage   : <b>{trade_leverage}x</b>\n"
                f"{'─'*34}\n"
                f"⏱ Auto-cancel kalau harga sentuh target TP duluan sebelum entry fill "
                f"(safety net: {LIMIT_ORDER_TIMEOUT_MINUTES/60:.0f} jam)"
            )
        print(f"⏳ [LIMIT ORDER] {symbol} | {direction} | Limit:{entry} | Lot:{lot} | OrderID:{_limit_order_id}")
        _note_order_success_streak()
        return True

    except Exception as e:
        err_str = str(e)
        print(f"❌ ERROR execute {symbol}: {e}")
        send_telegram_raw(f"❌ <b>ERROR ENTRY</b> {symbol}\n<code>{e}</code>")
        # Jangan record_pair_fail untuk -1111 (precision) — sudah di-retry di atas.
        # Jangan record_pair_fail untuk -1007 / network error yang SUDAH diverifikasi
        # TIDAK ADA order di Binance — itu infra hiccup sesaat, bukan indikasi pair
        # ini bermasalah. Kalau tetap dihitung ke fail-counter, pair bisa ter-cooldown
        # 6 jam padahal setupnya sendiri valid, cuma kena timeout API sekali.
        # CATATAN: kasus "verifikasi status juga gagal" (kita tidak tahu order ada
        # atau tidak) TETAP dihitung ke fail-counter — karena di situ kita punya
        # ketidakpastian ganda (order gagal + verifikasi gagal), lebih aman anggap
        # ada masalah nyata dan biarkan fail-counter jalan seperti biasa.
        # record_pair_fail hanya untuk error fundamental (balance, margin, minNotional, dll)
        _is_transient = (
            "-1111" in err_str or "precision" in err_str.lower()
            or "terverifikasi tidak ada order" in err_str
        )
        if not _is_transient:
            record_pair_fail(symbol)
        return False



# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 18b — PENDING LIMIT ORDER MONITOR
# ═══════════════════════════════════════════════════════════════════════════

def _activate_position_from_limit(symbol: str, pend: dict):
    """
    Dipanggil saat limit order terisi (status FILLED).
    Pasang SL + TP lalu pindahkan ke active_positions.
    """
    entry         = pend["entry"]
    sl            = pend["sl"]
    tp1           = pend["tp1"]
    tp2           = pend["tp2"]
    lot           = pend["lot"]
    side          = pend["side"]
    sl_side       = pend["sl_side"]
    direction     = pend["direction"]
    position_side = pend["position_side"]
    tick_size     = pend["tick_size"]
    step_size     = pend["step_size"]
    mkt_step_size = pend["mkt_step_size"]
    min_qty       = pend["min_qty"]
    eff_prec      = pend["eff_prec"]
    mode          = pend["mode"]
    signal        = pend["signal"]

    # ── TP1 PARTIAL + TP2 (diaktifkan lagi, permintaan user) ────────────────
    # tp1_size = TP1_PARTIAL dari lot (default 50%), sisanya tp2_size ke TP2.
    tp1_size = round_lot_to_step(lot * TP1_PARTIAL, mkt_step_size)
    if tp1_size < min_qty:
        tp1_size = round_lot_to_step(min_qty, mkt_step_size)
    tp1_size = min(tp1_size, lot)
    tp2_size = round_lot_to_step(lot - tp1_size, mkt_step_size)
    if 0 < tp2_size < min_qty:
        # sisa lot lebih kecil dari min_qty exchange — gabungkan semua ke TP1
        # (tidak bisa pasang order TP2 di bawah min_qty), TP2 order tidak dipasang.
        tp1_size = lot
        tp2_size = 0.0
    print(f"  🎯 [{symbol}] TP1/TP2: tp1_size={tp1_size} @ {tp1} | tp2_size={tp2_size} @ {tp2}")

    # Adjust SL/TP dari harga fill fresh
    fresh_price = get_current_price(symbol) or entry
    def rtick(p): return round_price_to_tick(p, tick_size)
    min_sl_buf = max(1, round(fresh_price * 0.005 / tick_size)) * tick_size if tick_size > 0 else fresh_price * 0.005
    min_tp_buf = max(1, round(fresh_price * 0.002 / tick_size)) * tick_size if tick_size > 0 else fresh_price * 0.002

    # FIX ROOT CAUSE "RR jelek pas eksekusi" (contoh nyata: DOTUSDT LONG,
    # SL 1.81% vs TP cuma 0.75% = RR~0.41 padahal signal asli RR 1.5-2.5):
    # blok ini dulu HANYA menjamin SL/TP ada di sisi yang benar & minimal
    # sejauh buffer kecil dari fresh_price — TIDAK PERNAH menjaga rasio RR
    # aslinya. Kalau limit order butuh waktu sampai FILLED dan harga sempat
    # bergerak, fresh_price bisa jauh dari entry sinyal awal: SL jadi
    # melebar (mengambil sisi "lebih jauh" via min()/max()) sementara TP
    # cuma digeser oleh min_tp_buf yang sangat kecil (0.2%) — akibatnya RR
    # asli kolaps drastis tanpa disadari sampai posisi sudah jalan.
    #
    # FIX v2 (root cause BARU ditemukan — kasus nyata: DOGEUSDT LONG,
    # entry fill 0.090740, SL 0.089627, TP asli 0.092410 (RR 1.50), tapi
    # setelah aktivasi TP jadi 0.091736 (RR EFEKTIF cuma ~0.89 terhadap
    # entry beneran)). ROOT CAUSE: revisi sebelumnya (di bawah, komentar
    # lama) MENGHITUNG _new_sl_dist dan tp2 dari fresh_price (harga pasar
    # SAAT AKTIVASI, yang sudah bergeser turun sedikit dari entry fill),
    # BUKAN dari entry (harga posisi BENERAN terbuka). Karena fresh_price
    # sempat dip di bawah entry (wajar — noise candle sesudah limit fill),
    # new_sl_dist (fresh_price→sl) jadi LEBIH PENDEK dari jarak entry→sl
    # yang asli, sehingga tp2 = fresh_price + new_sl_dist*orig_rr ikut
    # menyusut — walau rasio RR "benar" relatif ke fresh_price, RR EFEKTIF
    # terhadap entry (titik posisi beneran terbuka) jatuh jauh di bawah
    # orig_rr. Posisi jalan dari entry, bukan dari fresh_price — jadi TP
    # WAJIB dihitung dari entry, bukan dari harga pasar sesaat setelahnya.
    #
    # Sekarang: hitung dulu RR asli dari sinyal (sl vs tp2 vs entry sinyal),
    # SL boleh disesuaikan (mis. dilebarkan sedikit demi guard jarak minimum
    # dari fresh_price — lihat min_sl_buf di bawah, ini tetap perlu supaya
    # SL tidak ketinggalan terlalu rapat dari harga saat order dipasang ke
    # exchange), TAPI tp1/tp2 SELALU di-re-derive dari (entry, sl_final,
    # orig_rr1/orig_rr2) — entry TETAP sebagai anchor, bukan fresh_price —
    # supaya RR aktual posisi (dihitung dari entry, bukan dari harga sesaat)
    # selalu >= RR yang lolos grading, tidak peduli fresh_price bergeser ke
    # mana. tp1 dan tp2 dijaga TERPISAH (rasio masing-masing terhadap SL
    # dipertahankan) supaya partial TP1 + trailing ke TP2 tetap berfungsi.
    _orig_sl_dist = abs(entry - sl)
    _orig_rr1 = (abs(tp1 - entry) / _orig_sl_dist) if _orig_sl_dist > 0 else RR_GRADE_B
    _orig_rr2 = (abs(tp2 - entry) / _orig_sl_dist) if _orig_sl_dist > 0 else RR_GRADE_B
    _orig_rr1 = min(max(_orig_rr1, RR_GRADE_B), 2.5)  # guard eksekusi (2026-11-10: floor ikut TP1_R_MULT, bukan 1.5 hard-coded)
    _orig_rr2 = min(max(_orig_rr2, 1.5), 4.0)  # tp2 boleh lebih jauh dari tp1
    # TP2 WAJIB minimal 1R lebih jauh dari TP1 (RR_GRADE_A + 0.5 = 2.5R vs
    # RR_GRADE_B = 1.5R, selisih 1.0R sesuai calculate_rr sinyal). Tanpa ini,
    # rr2 asli yang sudah kena clamp bisa nyaris sama dengan rr1.
    _orig_rr2 = max(_orig_rr2, _orig_rr1 + (RR_GRADE_A + 0.5 - RR_GRADE_B))

    if direction == "LONG":
        sl = rtick(min(sl, fresh_price - min_sl_buf))
        # Anchor jarak RR ke ENTRY (posisi beneran terbuka di sini), bukan
        # ke fresh_price. FIX: fresh_price TIDAK lagi dipakai untuk menggeser
        # TP. Sebelumnya max(entry+dist*rr, fresh_price+min_tp_buf) menarik
        # TP1 ke fresh_price saat harga sudah bergerak jauh ke arah TP sebelum
        # order terisi -> TP1 dan TP2 menumpuk di harga yang sama (bug kartu
        # ATOMUSDT: TP1 1.6440 vs TP2 1.6430, selisih 1 tick). TP kini murni
        # entry + jarak SL x RR sinyal, sama dengan yang lolos grading.
        _new_sl_dist = abs(entry - sl)
        tp1 = rtick(entry + _new_sl_dist * _orig_rr1)
        tp2 = rtick(entry + _new_sl_dist * _orig_rr2)
        # KEAMANAN SISI (bukan penggeser RR): kalau harga pasar SUDAH melewati
        # TP1 saat order terisi, order TP ditolak Binance (would immediately
        # trigger) -> 5x gagal -> emergency close. Dorong TP1 ke sisi valid
        # (fresh_price + buffer) dan geser TP2 ikut agar jarak TP1->TP2 tetap
        # >= 1R. Hanya aktif kalau TP memang sudah tidak valid; normalnya TP
        # tetap persis 1.5R/2.5R dari sinyal.
        _tp_gap = _new_sl_dist * (RR_GRADE_A + 0.5 - RR_GRADE_B)
        if tp1 < fresh_price + min_tp_buf:
            tp1 = rtick(fresh_price + min_tp_buf)
            tp2 = max(tp2, rtick(tp1 + _tp_gap))
    else:
        sl = rtick(max(sl, fresh_price + min_sl_buf))
        _new_sl_dist = abs(sl - entry)
        tp1 = rtick(entry - _new_sl_dist * _orig_rr1)
        tp2 = rtick(entry - _new_sl_dist * _orig_rr2)
        _tp_gap = _new_sl_dist * (RR_GRADE_A + 0.5 - RR_GRADE_B)
        if tp1 > fresh_price - min_tp_buf:
            tp1 = rtick(fresh_price - min_tp_buf)
            tp2 = min(tp2, rtick(tp1 - _tp_gap))
    print(f"  🔧 [{symbol}] RR preserved on fill-adjust (anchor=entry): "
          f"orig_rr1={_orig_rr1:.2f} orig_rr2={_orig_rr2:.2f} entry={entry} sl={sl} "
          f"new_sl_dist={_new_sl_dist:.6f} → tp1={tp1} tp2={tp2}")

    # ── GUARD RR FLOOR (final, pasca semua adjust di atas) ──────────────────
    # Lapis terakhir: hitung ulang RR SEBENARNYA dari entry/sl/tp final di
    # atas (setelah rtick, setelah guard SL invalid di bawah BELUM jalan —
    # jadi kalau guard itu nanti masih menggeser sl, cek ini perlu diulang;
    # lihat pemanggilan ke-2 di bawah). Kalau ternyata masih < RR_GRADE_B
    # (mis. karena pembulatan tick sangat kasar utk pair harga rendah),
    # paksa TP naik lagi supaya RR minimal RR_GRADE_B tetap terjaga —
    # tidak pernah membiarkan posisi jalan dengan RR di bawah minimum grade.
    def _enforce_rr_floor(entry_, sl_, tp_, direction_):
        _d = abs(entry_ - sl_)
        if _d <= 0:
            return tp_
        _rr_now = abs(tp_ - entry_) / _d
        if _rr_now < RR_GRADE_B:
            print(f"  ⚠️  [{symbol}] RR pasca fill-adjust {_rr_now:.2f} < {RR_GRADE_B} — paksa TP naik ke RR minimum")
            if direction_ == "LONG":
                return rtick(entry_ + _d * RR_GRADE_B)
            else:
                return rtick(entry_ - _d * RR_GRADE_B)
        return tp_

    tp1 = _enforce_rr_floor(entry, sl, tp1, direction)
    # TP2 pakai floor RR SWING (RR_GRADE_A + 0.5), bukan RR_GRADE_B yang sama
    # dengan TP1 — memanggil _enforce_rr_floor (floor RR_GRADE_B) untuk
    # keduanya adalah bug yang bikin TP1==TP2 saat SL melebar habis-habisan
    # (kasus nyata: DOGEUSDT/GMXUSDT, RR1 & RR2 sama-sama jatuh ke floor yang
    # identik). _enforce_rr_floor generik dipakai lagi di sini dengan
    # RR_GRADE_A + 0.5 supaya TP2 tetap punya target lebih jauh dari TP1.
    _d2 = abs(entry - sl)
    if _d2 > 0 and (abs(tp2 - entry) / _d2) < (RR_GRADE_A + 0.5):
        print(f"  ⚠️  [{symbol}] RR2 pasca fill-adjust {(abs(tp2 - entry) / _d2):.2f} < {RR_GRADE_A + 0.5} (target swing) — paksa TP2 naik")
        tp2 = rtick(entry + _d2 * (RR_GRADE_A + 0.5)) if direction == "LONG" else rtick(entry - _d2 * (RR_GRADE_A + 0.5))
    # tp2 tidak boleh lebih dekat dari tp1 (bisa terjadi kalau enforce_rr_floor
    # menaikkan tp1 tapi tp2 sudah lebih dulu di atas floor).
    if direction == "LONG":
        tp2 = max(tp2, tp1)
    else:
        tp2 = min(tp2, tp1)
    # jaring pengaman terakhir: kalau tp2 masih persis == tp1 (mis. rtick
    # membulatkan keduanya ke tick yang sama untuk pair harga sangat rendah),
    # dorong tp2 minimal setengah RR_GRADE_B lebih jauh supaya partial
    # TP1/TP2 tidak pernah jadi single-TP tersamar.
    _min_gap = _d2 * (RR_GRADE_A + 0.5 - RR_GRADE_B)   # 1R, sama dgn selisih TP1->TP2 di calculate_rr
    if _d2 > 0 and abs(tp2 - tp1) < _min_gap - tick_size:
        tp2 = rtick(tp1 + _min_gap) if direction == "LONG" else rtick(tp1 - _min_gap)

    # ── FIX: validasi SL — cegah harga SL invalid (<=0 atau salah sisi) ──────
    # ROOT CAUSE bug "SL BELUM TERPASANG" berulang TANPA HENTI (algoOrder 400
    # "Stop price less than zero", stopPrice=-0.0 di STOP_MARKET fallback):
    # sl dari sinyal (pend["sl"]) kadang sudah rusak/negatif — misalnya
    # zone_bottom - buffer ATR yang jatuh di bawah nol untuk koin harga
    # rendah (contoh nyata: OPUSDT, ribuan attempt gagal berjam-jam).
    # min(sl, fresh_price - min_sl_buf) tetap mengambil nilai negatif itu
    # apa adanya → retry loop di bawah SELALU mengirim harga yang sama-sama
    # rusak, jadi TIDAK PERNAH bisa berhasil walau di-retry berapa kali pun.
    # Solusi: kalau hasil akhir sl tidak valid, fallback ke persentase aman
    # dari harga fresh saat ini.
    _sl_fallback_pct = 0.015  # 1.5% dari harga — dipakai HANYA jika sl asli invalid
    _sl_was_fallback = False
    if direction == "LONG":
        if sl <= 0 or sl >= fresh_price:
            print(f"  ⚠️  [{symbol}] SL invalid (sl={sl}, fresh_price={fresh_price}) — pakai fallback {_sl_fallback_pct*100:.1f}% dari harga")
            sl = rtick(fresh_price * (1 - _sl_fallback_pct))
            _sl_was_fallback = True
    else:
        if sl <= 0 or sl <= fresh_price:
            print(f"  ⚠️  [{symbol}] SL invalid (sl={sl}, fresh_price={fresh_price}) — pakai fallback {_sl_fallback_pct*100:.1f}% dari harga")
            sl = rtick(fresh_price * (1 + _sl_fallback_pct))
            _sl_was_fallback = True

    # sl baru saja bisa berubah lewat fallback di atas — tp1/tp2 yang dihitung
    # sebelumnya jadi tidak lagi konsisten dengan sl ini (RR bisa berubah
    # ke arah manapun). Re-enforce floor sekali lagi supaya RR final tetap
    # terjamin >= RR_GRADE_B walau lewat jalur fallback SL invalid.
    if _sl_was_fallback:
        tp1 = _enforce_rr_floor(entry, sl, tp1, direction)
        tp2 = _enforce_rr_floor(entry, sl, tp2, direction)
        if direction == "LONG":
            tp2 = max(tp2, tp1)
        else:
            tp2 = min(tp2, tp1)

    if SINGLE_TP_MODE:
        tp2 = tp1
    print(f"  📐 SL/TP FINAL [{symbol}] sl={sl:.{eff_prec}f} tp1={tp1:.{eff_prec}f} tp2={tp2:.{eff_prec}f}")

    def _emergency_close():
        """
        Tutup posisi darurat via MARKET order.
        Multi-attempt dengan fallback presisi makin ketat:
          1. Pakai mkt_stepSize + quantityPrecision dari filter (re-fetch fresh)
          2. Jika gagal precision (-1111): floor ke integer (quantityPrecision=0)
          3. Jika masih gagal: coba closePosition=true (tanpa quantity)
        """
        em_filters  = get_lot_filters(symbol)
        em_step     = em_filters.get("mkt_stepSize", em_filters.get("stepSize", step_size))
        em_qty_prec = em_filters.get("quantityPrecision", None)

        # Derive qty_prec dari em_step jika tidak tersedia (testnet sering tidak return)
        if em_qty_prec is None and em_step > 0:
            em_qty_prec = max(0, round(-math.log10(em_step)))

        def _build_params(qty_str: str, use_close_position: bool = False) -> dict:
            p = {"symbol": symbol, "side": sl_side, "type": "MARKET"}
            if use_close_position:
                # FIX: closePosition=true HARUS tetap menyertakan positionSide
                # kalau akun HEDGE MODE. Asumsi lama ("closePosition tidak boleh
                # digabung positionSide") cuma berlaku di ONE-WAY mode — di
                # HEDGE mode, closePosition=true TANPA positionSide justru
                # di-reject 400 oleh Binance karena tidak tahu leg LONG/SHORT
                # mana yang harus ditutup. Inilah penyebab error di screenshot.
                p["closePosition"] = "true"
                if position_side != "BOTH":
                    p["positionSide"] = position_side
            else:
                p["quantity"] = qty_str
                if position_side != "BOTH":
                    p["positionSide"] = position_side
                else:
                    p["reduceOnly"] = "true"
            return p

        # ── FIX v39b: Force sync server time sebelum semua attempt ─────────
        # Emergency close adalah order paling kritis — tidak boleh gagal 400.
        # Cache offset mungkin stale (5 menit), paksa sync ulang sekarang.
        _sync_server_time(force=True)

        # ── Attempt 1: floor ke quantityPrecision yang ter-derive ────────────
        em_lot = lot
        if em_qty_prec is not None:
            em_lot = round(math.floor(em_lot * (10 ** em_qty_prec)) / (10 ** em_qty_prec), em_qty_prec)
        em_qty = format_qty(em_lot, em_step, em_qty_prec)
        try:
            api_post("/fapi/v1/order", _build_params(em_qty))
            print(f"  🚨 [{symbol}] EMERGENCY CLOSE berhasil (attempt 1) qty={em_qty}")
            send_telegram_raw(f"🚨 <b>EMERGENCY CLOSE — {symbol}</b>\nPosisi ditutup otomatis karena SL/TP gagal dipasang.")
            return
        except Exception as ec1:
            print(f"  ⚠️  [{symbol}] EMERGENCY attempt 1 gagal (qty={em_qty}): {ec1}")

        # ── Attempt 2: floor ke integer (quantityPrecision=0) ────────────────
        em_lot_int = math.floor(lot)
        em_qty_int = str(int(em_lot_int))
        if em_lot_int > 0:
            try:
                api_post("/fapi/v1/order", _build_params(em_qty_int))
                print(f"  🚨 [{symbol}] EMERGENCY CLOSE berhasil (attempt 2, integer qty={em_qty_int})")
                send_telegram_raw(f"🚨 <b>EMERGENCY CLOSE — {symbol}</b>\nPosisi ditutup (qty integer={em_qty_int}) karena SL/TP gagal dipasang.")
                return
            except Exception as ec2:
                print(f"  ⚠️  [{symbol}] EMERGENCY attempt 2 gagal (qty={em_qty_int}): {ec2}")

        # ── Attempt 3: closePosition=true (tanpa quantity, Binance tutup semua) ──
        try:
            api_post("/fapi/v1/order", _build_params("", use_close_position=True))
            print(f"  🚨 [{symbol}] EMERGENCY CLOSE berhasil (attempt 3, closePosition=true)")
            send_telegram_raw(f"🚨 <b>EMERGENCY CLOSE — {symbol}</b>\nPosisi ditutup via closePosition karena SL/TP gagal dipasang.")
            return
        except Exception as ec3:
            print(f"  ❌ [{symbol}] EMERGENCY CLOSE GAGAL semua attempt: {ec3}")
            send_telegram_raw(f"🆘 <b>EMERGENCY CLOSE GAGAL — {symbol}</b>\nCek dan tutup manual di Binance!\nError: <code>{ec3}</code>")

    # ── Pasang SL ──
    # FIX v63: SL WAJIB terpasang — TIDAK fallback ke emergency close lagi,
    # karena emergency close (MARKET) sering juga gagal/tidak benar2 menutup
    # posisi (lihat _emergency_close attempt 1-3 yang bisa semuanya gagal).
    # Sekarang: retry TANPA BATAS sampai SL berhasil terpasang. User tetap
    # diberi notifikasi Telegram saat error (pertama kali, lalu tiap 10x)
    # supaya tahu posisi masih "telanjang" sementara retry berjalan.
    _sync_server_time(force=True)
    sl_ok = False
    _sl_attempt = 0
    _sl_last_notify = 0
    _sl_order_ref = None
    while not sl_ok:
        _sl_attempt += 1
        try:
            _sl_order_ref = algo_post_sl(symbol, sl_side, sl, quantity=lot, position_side=position_side)
            sl_ok = True
            break
        except Exception as _e:
            print(f"  ⚠️  [{symbol}] SL attempt {_sl_attempt} gagal: {_e}")
            if _sl_attempt == 1 or (_sl_attempt - _sl_last_notify) >= 10:
                _sl_last_notify = _sl_attempt
                send_telegram_raw(
                    f"⚠️ <b>SL BELUM TERPASANG — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Percobaan ke-{_sl_attempt} gagal.\n"
                    f"Error: <code>{_e}</code>\n"
                    f"🔁 Bot terus mencoba pasang SL sampai berhasil "
                    f"(posisi TIDAK akan di-close paksa)."
                )
            _sync_server_time(force=True)  # re-sync tiap gagal, bukan pakai offset lama

            # ── FIX: self-heal — jangan terus kirim harga SL yang sama kalau
            # ternyata sudah tidak valid lagi (harga pasar sudah bergerak jauh,
            # atau — seperti kasus OPUSDT — sl awal memang rusak/negatif).
            # Tanpa ini, retry di atas bisa berjalan RIBUAN kali dan tetap
            # selalu gagal karena mengirim stopPrice yang sama persis.
            try:
                _sl_fresh = get_current_price(symbol)
                if _sl_fresh:
                    _needs_recalc = (
                        sl <= 0 or
                        (direction == "LONG"  and sl >= _sl_fresh) or
                        (direction == "SHORT" and sl <= _sl_fresh)
                    )
                    if _needs_recalc:
                        _old_sl = sl
                        _pct = 0.015
                        sl = rtick(_sl_fresh * (1 - _pct)) if direction == "LONG" else rtick(_sl_fresh * (1 + _pct))
                        print(f"  🔧 [{symbol}] SL di-recalc otomatis: {_old_sl} → {sl} (harga sekarang {_sl_fresh})")
            except Exception as _e2:
                print(f"  ⚠️  [{symbol}] gagal recalc SL saat retry: {_e2}")

            # Backoff: 0.2s flat untuk 5 attempt pertama, lalu naik bertahap (cap 5s)
            _sleep_s = 0.2 if _sl_attempt <= 5 else min(5.0, 0.5 * (_sl_attempt - 4))
            time.sleep(_sleep_s)

    if _sl_attempt > 1:
        send_telegram_raw(
            f"✅ <b>SL BERHASIL TERPASANG — {symbol}</b>\n"
            f"Setelah {_sl_attempt} percobaan. Posisi sekarang aman (SL aktif)."
        )

    # ── Pasang TP1 (partial) ──────────────────────────────────────────────
    _sync_server_time(force=True)
    tp_ok = False
    _tp_order_ref = None
    for _a in range(5):
        try:
            _tp_order_ref = algo_post_tp(symbol, sl_side, tp1, tp1_size, position_side=position_side)
            tp_ok = True
            break
        except Exception as _e:
            print(f"  ⚠️  [{symbol}] TP1 target attempt {_a+1}/5: {_e}")
            if _a < 4:
                _sync_server_time(force=True)
                time.sleep(0.2)
    if not tp_ok:
        try:
            cancel_open_orders(symbol)
        except Exception:
            pass
        send_telegram_raw(f"❌ <b>TP TARGET GAGAL — {symbol}</b>\nEmergency close dimulai...")
        _emergency_close()
        set_cooldown_post_close(symbol, reason="TP target gagal setelah limit fill")
        return

    # ── Pasang TP2 (sisa lot) — dipasang di awal juga, bukan cuma nunggu
    # self-heal, supaya sisa lot langsung terlindungi sejak posisi terbuka. ──
    _tp2_order_ref = None
    if tp2_size > 0:
        _tp2_ok = False
        for _a in range(5):
            try:
                _tp2_order_ref = algo_post_tp(symbol, sl_side, tp2, tp2_size, position_side=position_side)
                _tp2_ok = True
                break
            except Exception as _e:
                print(f"  ⚠️  [{symbol}] TP2 target attempt {_a+1}/5: {_e}")
                if _a < 4:
                    _sync_server_time(force=True)
                    time.sleep(0.2)
        if not _tp2_ok:
            # TP1 sudah terpasang dan posisi sudah dianggap live — jangan
            # emergency-close cuma karena TP2 gagal; self-heal (lihat
            # manage_trailing) akan mencoba pasang ulang tiap scan berikutnya.
            print(f"  ⚠️  [{symbol}] TP2 gagal dipasang setelah 5 percobaan — akan di-self-heal saat monitoring jalan.")
            send_telegram_raw(
                f"⚠️ <b>TP2 BELUM TERPASANG — {symbol}</b>\n"
                f"TP1 sudah aman, tapi TP2 untuk sisa lot gagal dipasang.\n"
                f"Bot akan terus mencoba (self-heal) saat monitoring berjalan."
            )

    # ── Daftarkan ke active_positions ──
    register_trade_filled(symbol)   # hitung trade harian (terisi = pakai jatah)
    with _positions_lock:   # FIX BUG #3: thread-safe write
        active_positions[symbol] = {
            "entry":           entry,
            "sl":              sl,
            "original_sl":     sl,      # ← simpan SL awal untuk kalkulasi RR trailing
            "tp1":             tp1,
            "tp2":             tp2,
            "lot":             lot,
            "tp1_size":        tp1_size,
            "tp2_size":        tp2_size,
            "side":            side,
            "sl_side":         sl_side,
            "direction":       direction,
            "position_side":   position_side,
            "tick_size":       tick_size,
            "eff_prec":        eff_prec,
            "step_size":       step_size,
            "mkt_step_size":   mkt_step_size,
            "min_qty":         min_qty,
            "tp1_hit":         False,
            "trailing_active": False,
            "open_time":       datetime.now(timezone.utc),
            "pre_existing":    False,
            "entry_type":      pend.get("entry_type", "📊 Bot"),
            # FIX: sebelumnya mode/entry_tf/score TIDAK disimpan di active_positions
            # sama sekali, jadi setiap trade close selalu ke-log dengan mode="?" dan
            # score=0 di trade_log.json (analytics jadi tidak berguna).
            "mode":            mode.get("label", "?") if isinstance(mode, dict) else str(mode),
            "entry_tf":        mode.get("entry_tf", "?") if isinstance(mode, dict) else "?",
            "score":           signal.get("score", 0) if isinstance(signal, dict) else 0,
            # FIX: simpan breakdown komponen skor (bos/liquidity/zone/volume/dst)
            # supaya saat trade close, breakdown ini ikut ke-log ke trade_log.json
            # dan bisa dianalisa per-komponen lewat /scorebd — bukan cuma total
            # score seperti /scorewr.
            "score_bd":        signal.get("_score_bd", {}) if isinstance(signal, dict) else {},
            # ── Konteks market saat entry — dibawa sampai ke trade log ───────
            "atr_pct":         signal.get("_atr_pct", 0) if isinstance(signal, dict) else 0,
            "sl_atr_mult":     signal.get("_sl_atr_mult", 0) if isinstance(signal, dict) else 0,
            "vol_rat":         signal.get("_vol_rat", 0) if isinstance(signal, dict) else 0,
            "mkt_regime":      signal.get("_mkt_regime", "?") if isinstance(signal, dict) else "?",
            "alt_bull_pct":    signal.get("_alt_bull_pct", -1) if isinstance(signal, dict) else -1,
            "alt_bear_pct":    signal.get("_alt_bear_pct", -1) if isinstance(signal, dict) else -1,
            "htf_bias":        signal.get("_htf_bias", "?") if isinstance(signal, dict) else "?",
            "bos_strength":    signal.get("_bos_strength", "?") if isinstance(signal, dict) else "?",
            "engulf_break":    signal.get("_engulf_break", "?") if isinstance(signal, dict) else "?",
            "engulf_entry":    signal.get("_engulf_entry", "?") if isinstance(signal, dict) else "?",
            "engulf_note":     signal.get("_engulf_note", "") if isinstance(signal, dict) else "",
            "fvg_retest":      signal.get("_fvg_retest", "?") if isinstance(signal, dict) else "?",
            "fvg_retest_note": signal.get("_fvg_retest_note", "") if isinstance(signal, dict) else "",
            "stoch_div":       signal.get("_stoch_div", "?") if isinstance(signal, dict) else "?",
            "stoch_weak":      signal.get("_stoch_weak", "?") if isinstance(signal, dict) else "?",
            "stoch_note":      signal.get("_stoch_note", "") if isinstance(signal, dict) else "",
            "sweep_strength":  signal.get("_sweep_strength", "?") if isinstance(signal, dict) else "?",
            "sl_source":       signal.get("_sl_source", "?") if isinstance(signal, dict) else "?",
            "rsi_entry":       signal.get("_rsi_entry", 0) if isinstance(signal, dict) else 0,
            "btc_bias":        signal.get("_btc_bias", "?") if isinstance(signal, dict) else "?",
            "btcd_trend":      signal.get("_btcd_trend", "?") if isinstance(signal, dict) else "?",
            "btc_ltf_dir":     signal.get("_btc_ltf_dir", "?") if isinstance(signal, dict) else "?",
            "btcd_ltf_dir":    signal.get("_btcd_ltf_dir", "?") if isinstance(signal, dict) else "?",
            # MAE/MFE water mark — diupdate tiap iterasi manage_trailing()
            "mae_pct":         0.0,   # max adverse excursion (%) sejak entry
            "mfe_pct":         0.0,   # max favorable excursion (%) sejak entry
            # FIX KRITIS — simpan referensi order asli (algoId/orderId) SL/TP1/TP2.
            # Ini yang bikin manage_trailing() bisa cek status order LANGSUNG ke
            # Binance (GET /fapi/v1/algoOrder) alih-alih menebak dari kombinasi
            # harga+lot, yang terbukti bisa salah simpul TP1 "sudah kena" padahal
            # harga belum pernah menyentuh level TP1 sama sekali.
            "sl_order_ref":    _sl_order_ref,
            "tp1_order_ref":   _tp_order_ref,
            "tp2_order_ref":   _tp2_order_ref,
        }
    save_state()   # FIX: simpan langsung ke disk agar posisi ini tidak jadi "HP" kalau bot restart

    _cd_direction = "BULLISH" if direction == "LONG" else "BEARISH"
    set_cooldown(symbol, mode["label"], mode["entry_tf"], direction=_cd_direction)

    signal_display = dict(signal)
    signal_display["entry"]       = entry
    signal_display["stop_loss"]   = sl
    signal_display["take_profit"] = [tp1, tp2]
    try:
        send_telegram_signal(
            signal       = signal_display,
            mode         = mode,
            score_bd     = signal.get("_score_bd", {}),
            session      = signal.get("_session", get_session()),
            pa_name      = signal.get("_pa_name", "-"),
            btc_bias     = signal.get("_btc_bias", "-"),
            btcd_trend   = signal.get("_btcd_trend", "-"),
            macro_reason = signal.get("_macro_reason", "-"),
            # Posisi SUDAH live (order fill + SL/TP sudah terpasang di titik
            # ini) — card TIDAK BOLEH dibatalkan diam-diam walau RR aktual
            # kebetulan di bawah minimum grade (lihat komentar di dalam
            # send_telegram_signal soal allow_rr_below_floor). User harus
            # selalu tahu status posisi live-nya, apa pun angka RR-nya.
            allow_rr_below_floor = True,
        )
    except Exception as _tg_err:
        print(f"  ⚠️  [{symbol}] send_telegram_signal gagal (posisi tetap aktif): {_tg_err}")
        # Fallback: posisi SUDAH live, jadi kalau card lengkap gagal terkirim
        # (exception apa pun), user TETAP harus dapat notifikasi minimal —
        # jangan biarkan kegagalan ini diam-diam tanpa jejak di Telegram.
        try:
            send_telegram_raw(
                f"⚠️ <b>SIGNAL CARD GAGAL DIKIRIM — {symbol}</b>\n"
                f"Posisi tetap AKTIF (SL/TP sudah terpasang), tapi detail sinyal "
                f"lengkap gagal ditampilkan.\n"
                f"Entry: {entry} | SL: {sl} | TP1: {tp1} | TP2: {tp2}\n"
                f"Error: <code>{_tg_err}</code>"
            )
        except Exception as _fallback_err:
            print(f"  ❌ [{symbol}] Fallback notif card-gagal juga gagal: {_fallback_err}")
    # Gunakan eff_prec untuk format harga di notif fill — lebih akurat dari tick_size
    _eff_p = pend.get("eff_prec", max(0, round(-math.log10(tick_size))) if tick_size > 0 else 4)
    def _fmt_fill(p: float) -> str:
        return f"{p:.{_eff_p}f}"

    # ── Estimasi Loss (kena SL) & Profit (kena TP1/TP2) dalam USDT + % ─────────
    # Dihitung dari notional riil (lot × harga) — merepresentasikan P&L
    # sebenarnya di akun, terlepas dari berapa leverage yang dipakai.
    if direction == "LONG":
        sl_pct  = (entry - sl)  / entry * 100
        tp1_pct = (tp1 - entry) / entry * 100
        tp2_pct = (tp2 - entry) / entry * 100
    else:  # SHORT
        sl_pct  = (sl - entry)  / entry * 100
        tp1_pct = (entry - tp1) / entry * 100
        tp2_pct = (entry - tp2) / entry * 100

    est_loss_usdt = abs(entry - sl) * lot
    est_tp1_usdt  = abs(tp1 - entry) * tp1_size
    est_tp2_usdt  = abs(tp2 - entry) * tp2_size
    est_tp_total_usdt = est_tp1_usdt + est_tp2_usdt

    if tp2_size > 0:
        tp1_lot_pct = round(tp1_size / lot * 100) if lot > 0 else 0
        tp2_lot_pct = round(tp2_size / lot * 100) if lot > 0 else 0
        send_telegram_raw(
            f"✅ <b>LIMIT ORDER TERISI — {symbol}</b>\n"
            f"{'─'*34}\n"
            f"{'🟢' if direction == 'LONG' else '🔴'} <b>{direction}</b> | Entry: <b>{_fmt_fill(entry)}</b>\n"
            f"🛑 SL: <b>{_fmt_fill(sl)}</b>\n"
            f"🎯 TP1: <b>{_fmt_fill(tp1)}</b> ({tp1_lot_pct}% lot) — SL geser ke BE begitu kena\n"
            f"🎯 TP2: <b>{_fmt_fill(tp2)}</b> ({tp2_lot_pct}% lot, sisa)\n"
            f"✅ SL + TP1 + TP2 sudah terpasang\n"
            f"{'─'*34}\n"
            f"📉 Estimasi Loss (SL kena, full lot): <b>-{est_loss_usdt:.2f} USDT</b> ({sl_pct:.2f}%)\n"
            f"📈 Estimasi Profit TP1: <b>+{est_tp1_usdt:.2f} USDT</b> ({tp1_pct:.2f}%)\n"
            f"📈 Estimasi Profit TP2: <b>+{est_tp2_usdt:.2f} USDT</b> ({tp2_pct:.2f}%)\n"
            f"💰 Total jika TP1+TP2 kena: <b>+{est_tp_total_usdt:.2f} USDT</b>"
        )
    else:
        send_telegram_raw(
            f"✅ <b>LIMIT ORDER TERISI — {symbol}</b>\n"
            f"{'─'*34}\n"
            f"{'🟢' if direction == 'LONG' else '🔴'} <b>{direction}</b> | Entry: <b>{_fmt_fill(entry)}</b>\n"
            f"🛑 SL: <b>{_fmt_fill(sl)}</b> | 🎯 Target TP: <b>{_fmt_fill(tp1)}</b>\n"
            f"✅ SL + TP tunggal sudah terpasang (100% lot)\n"
            f"{'─'*34}\n"
            f"📉 Estimasi Loss (SL kena): <b>-{est_loss_usdt:.2f} USDT</b> ({sl_pct:.2f}%)\n"
            f"📈 Estimasi Profit Target TP: <b>+{est_tp1_usdt:.2f} USDT</b> ({tp1_pct:.2f}%)\n"
            f"💰 Total jika target TP kena: <b>+{est_tp1_usdt:.2f} USDT</b>"
        )
    print(f"✅ [LIMIT FILLED] {symbol} | {direction} | Entry:{entry} | Lot:{lot}")


def monitor_pending_limit_orders():
    """
    Cek status semua pending limit order setiap scan loop.
    - FILLED  → aktifkan posisi (pasang SL/TP, masuk active_positions)
    - CANCELED/EXPIRED → bersihkan dict
    - Timeout (> LIMIT_ORDER_TIMEOUT_MINUTES) → cancel order di Binance
    """
    if not pending_limit_orders:
        return

    from datetime import timedelta
    now = datetime.now(timezone.utc)

    for symbol in list(pending_limit_orders.keys()):
        pend = pending_limit_orders.get(symbol)
        if pend is None:
            continue

        order_id  = pend.get("order_id")
        placed_at = pend.get("placed_at")
        direction = pend.get("direction", "")


        # ── FIX BARU: kalau harga sudah nyentuh TP1 duluan SEBELUM entry
        # ke-fill, setup-nya sudah tidak valid lagi (order limit ketinggalan
        # zaman — harga sudah lewat ke area take-profit tanpa pernah masuk).
        # Cancel limit order-nya sekarang, jangan tunggu timeout normal.
        # GUARD: cek status order dulu (bisa jadi sudah FILLED/PARTIALLY_FILLED
        # tepat sebelum harga nyampe TP1) — jangan sampai cancel order yang
        # posisinya sudah/sedang terbuka, itu beda kasus (harus di-manage_trailing).
        _tp1_chk = pend.get("tp1")
        if _tp1_chk:
            _cur_px = get_current_price(symbol)
            if _cur_px:
                _tp1_touched = (direction == "LONG"  and _cur_px >= _tp1_chk) or \
                               (direction == "SHORT" and _cur_px <= _tp1_chk)
                if _tp1_touched:
                    _tp1_status = None
                    if order_id:
                        try:
                            _tp1_ord = api_get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
                            _tp1_status = _tp1_ord.get("status", "")
                        except Exception as _tp1_qe:
                            print(f"  ⚠️  [{symbol}] Gagal cek status order sebelum TP1-cancel: {_tp1_qe}")
                    if _tp1_status == "FILLED":
                        # Race — sempat fill duluan tepat sebelum harga sampai TP1.
                        # Biarkan jalur normal (aktivasi posisi) yang urus, jangan cancel.
                        print(f"  ℹ️  [{symbol}] Harga sentuh TP1 tapi order sudah FILLED — aktifkan sebagai posisi, bukan cancel")
                    elif _tp1_status == "PARTIALLY_FILLED":
                        # Sudah ada isi sebagian — biarkan sisanya diproses jalur
                        # PARTIALLY_FILLED yang sudah ada (cek positionRisk & aktifkan).
                        print(f"  ℹ️  [{symbol}] Harga sentuh TP1 tapi order PARTIALLY_FILLED — biarkan jalur partial fill yang urus")
                    else:
                        print(f"  🚫 [{symbol}] Harga ({_cur_px}) sudah sentuh TP1 ({_tp1_chk}) sebelum entry ke-fill — cancel limit order")
                        try:
                            if order_id:
                                try:
                                    api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
                                except Exception:
                                    pass  # mungkin sudah tidak ada / sudah ke-cancel, aman diabaikan
                            cancel_open_orders(symbol)  # jaga-jaga ada conditional order nyangkut
                        except Exception as _tp1_ce:
                            print(f"  ⚠️  [{symbol}] Gagal cancel order saat TP1-precheck: {_tp1_ce}")
                        if symbol in pending_limit_orders:
                            del pending_limit_orders[symbol]
                        send_telegram_raw(
                            f"🚫 <b>LIMIT DIBATALKAN — TP1 KESENTUH DULUAN — {symbol}</b>\n"
                            f"{'─'*34}\n"
                            f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry rencana: {pend.get('entry')}\n"
                            f"🎯 TP1: {_tp1_chk} | Harga sekarang: {_cur_px}\n"
                            f"⚠️ Harga sudah lewat ke area TP1 tanpa entry ke-fill — setup dianggap invalid, order dibatalkan.\n"
                            f"Bot akan cari sinyal baru."
                        )
                        continue

        # ── Pastikan placed_at selalu timezone-aware UTC ────────────────────
        if isinstance(placed_at, datetime) and placed_at.tzinfo is None:
            placed_at = placed_at.replace(tzinfo=timezone.utc)

        # ── Cek timeout ──────────────────────────────────────────────────────
        try:
            elapsed_min = (now - placed_at).total_seconds() / 60 if placed_at else 999
        except TypeError:
            elapsed_min = 999  # fallback jika tipe datetime tidak kompatibel

        if elapsed_min >= LIMIT_ORDER_TIMEOUT_MINUTES:
            print(f"  ⏰ [{symbol}] Limit order timeout ({elapsed_min:.0f} menit) — cek status dulu sebelum cancel")
            # ── CEK STATUS DULU — jangan langsung cancel, bisa saja sudah FILLED ──────────────
            # Root cause bug: bot langsung DELETE tanpa cek status → kalau sudah FILLED
            # posisi terbuka tapi SL/TP tidak pernah dipasang karena pending dihapus.
            _timeout_status = None
            if order_id:
                try:
                    _ord = api_get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
                    _timeout_status = _ord.get("status", "")
                    print(f"  🔍 [{symbol}] Status order saat timeout: {_timeout_status}")
                except Exception as _sq:
                    print(f"  ⚠️  [{symbol}] Gagal query status saat timeout: {_sq}")

            if _timeout_status == "FILLED":
                # ── Posisi sudah terisi — aktifkan seperti biasa, jangan cancel ──────────────
                print(f"  ✅ [{symbol}] Order FILLED saat timeout — aktifkan posisi + pasang SL/TP")
                del pending_limit_orders[symbol]
                try:
                    _activate_position_from_limit(symbol, pend)
                    send_telegram_raw(
                        f"⚠️ <b>LIMIT FILLED SAAT TIMEOUT — {symbol}</b>\n"
                        f"{'─'*34}\n"
                        f"Order terisi tepat saat timeout ({elapsed_min:.0f} menit).\n"
                        f"Posisi diaktifkan — SL/TP dipasang otomatis."
                    )
                except Exception as _ae:
                    print(f"  ❌ [{symbol}] Error aktivasi posisi (timeout+filled): {_ae}")
                    send_telegram_raw(f"❌ <b>ERROR AKTIVASI (TIMEOUT+FILLED) — {symbol}</b>\n<code>{_ae}</code>")
                continue

            elif _timeout_status in ("CANCELED", "EXPIRED", "REJECTED"):
                # Sudah tidak aktif di Binance, bersihkan saja
                print(f"  🗑  [{symbol}] Order sudah {_timeout_status} di Binance — bersihkan")
                try:
                    cancel_open_orders(symbol)
                except Exception:
                    pass
                del pending_limit_orders[symbol]
                send_telegram_raw(
                    f"🗑 <b>LIMIT ORDER {_timeout_status} — {symbol}</b>\n"
                    f"Order sudah tidak aktif. Bot akan cari sinyal baru."
                )
                continue

            else:
                # NEW / PARTIALLY_FILLED / None (query gagal) → cancel sekarang
                print(f"  ⏰ [{symbol}] Timeout — cancel order {order_id} (status: {_timeout_status})")
                if order_id:
                    try:
                        api_delete("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
                        print(f"  🗑  [{symbol}] Limit order {order_id} berhasil di-cancel")
                    except Exception as _ce:
                        print(f"  ⚠️  [{symbol}] Gagal cancel limit order: {_ce}")
                # Bersihkan semua open/conditional orders
                try:
                    cancel_open_orders(symbol)
                    print(f"  🗑  [{symbol}] Semua open/conditional orders di-cancel (timeout)")
                except Exception as _ce2:
                    print(f"  ⚠️  [{symbol}] Gagal cancel open orders: {_ce2}")
                del pending_limit_orders[symbol]
                dir_str = "LONG" if direction == "LONG" else "SHORT"
                send_telegram_raw(
                    f"⏰ <b>LIMIT ORDER EXPIRED (SAFETY NET) — {symbol}</b>\n"
                    f"{'─'*34}\n"
                    f"Order {dir_str} tidak terisi dalam <b>{LIMIT_ORDER_TIMEOUT_MINUTES/60:.0f} jam</b> "
                    f"(TP1 juga belum kesentuh — ini jaring pengaman, bukan siklus normal).\n"
                    f"Limit order + semua conditional orders di-cancel otomatis.\n"
                    f"Bot akan cari sinyal baru."
                )
                continue

        # ── Cek status order di Binance ──────────────────────────────────────
        if not order_id:
            del pending_limit_orders[symbol]
            continue
        try:
            ord_data = api_get("/fapi/v1/order", {"symbol": symbol, "orderId": order_id}, signed=True)
            status   = ord_data.get("status", "")
        except Exception as _qe:
            print(f"  ⚠️  [{symbol}] Gagal query limit order status: {_qe}")
            # ── FIX v34+: Jangan langsung continue saat query 400/gagal ──────────
            # Root cause bug: query /fapi/v1/order return 400 (timestamp drift) →
            # bot skip → posisi sudah FILLED tapi SL/TP tidak pernah dipasang.
            # Fallback: cek /fapi/v2/positionRisk, kalau posisi sudah terbuka →
            # aktifkan langsung tanpa perlu status order.
            try:
                _fb_positions = api_get("/fapi/v2/positionRisk", signed=True)
                _fb_open = [
                    p for p in _fb_positions
                    if p["symbol"] == symbol and abs(float(p.get("positionAmt", 0))) > 0
                ]
                if _fb_open:
                    print(f"  ✅ [{symbol}] Query order gagal tapi posisi AKTIF terdeteksi via positionRisk — aktifkan + pasang SL/TP")
                    del pending_limit_orders[symbol]
                    send_telegram_raw(
                        f"⚠️ <b>FALLBACK AKTIVASI — {symbol}</b>\n"
                        f"{'─'*34}\n"
                        f"Query order status gagal (400/error) tapi posisi sudah terbuka.\n"
                        f"Aktivasi otomatis via positionRisk fallback — SL/TP akan dipasang."
                    )
                    try:
                        _activate_position_from_limit(symbol, pend)
                    except Exception as _fb_ae:
                        print(f"  ❌ [{symbol}] Error aktivasi fallback: {_fb_ae}")
                        send_telegram_raw(f"❌ <b>ERROR AKTIVASI FALLBACK — {symbol}</b>\n<code>{_fb_ae}</code>")
                    continue
                else:
                    print(f"  ℹ️  [{symbol}] Query gagal + tidak ada posisi terbuka — skip (order mungkin belum fill)")
            except Exception as _fb_err:
                print(f"  ⚠️  [{symbol}] Fallback positionRisk juga gagal: {_fb_err}")
            continue

        if status == "FILLED":
            print(f"  ✅ [{symbol}] Limit order FILLED — aktifkan posisi")
            del pending_limit_orders[symbol]
            try:
                _activate_position_from_limit(symbol, pend)
            except Exception as _ae:
                print(f"  ❌ [{symbol}] Error aktivasi posisi dari limit fill: {_ae}")
                send_telegram_raw(f"❌ <b>ERROR AKTIVASI LIMIT FILL — {symbol}</b>\n<code>{_ae}</code>")
                # ── FIX RISIKO 1: posisi sudah terbuka di Binance tapi aktivasi crash ──
                # _activate_position_from_limit mungkin belum sempat pasang SL sama sekali.
                # Cek apakah posisi benar-benar ada; jika ya, coba pasang SL darurat
                # agar posisi tidak naked. Jika SL gagal → emergency close via MARKET.
                try:
                    _em_positions = api_get("/fapi/v2/positionRisk", signed=True)
                    _em_open = [p for p in _em_positions if p["symbol"] == symbol and abs(float(p.get("positionAmt", 0))) > 0]
                    if _em_open:
                        _em_p       = _em_open[0]
                        _em_amt     = float(_em_p["positionAmt"])
                        _em_dir     = "LONG" if _em_amt > 0 else "SHORT"
                        _em_sl_side = "SELL" if _em_dir == "LONG" else "BUY"
                        _em_entry   = float(_em_p.get("entryPrice", pend["entry"]))
                        _em_lot     = abs(_em_amt)
                        _em_mark    = get_current_price(symbol) or _em_entry
                        _em_tick    = pend.get("tick_size", 0.01)
                        _em_step    = pend.get("step_size", 0.001)
                        _em_qty_prec = get_lot_filters(symbol).get("quantityPrecision", None)
                        # Coba pasang SL darurat (1.5% dari mark price)
                        if _em_dir == "LONG":
                            _em_sl_price = round_price_to_tick(_em_mark * 0.985, _em_tick)
                        else:
                            _em_sl_price = round_price_to_tick(_em_mark * 1.015, _em_tick)
                        if _em_qty_prec is not None:
                            _em_lot = round(math.floor(_em_lot * (10 ** _em_qty_prec)) / (10 ** _em_qty_prec), _em_qty_prec)
                        _pos_side = _em_p.get("positionSide", "BOTH")
                        print(f"  🚨 [{symbol}] Posisi naked terdeteksi pasca-error aktivasi — pasang SL darurat di {_em_sl_price}")
                        # FIX v63: jangan emergency-close — retry SL darurat sampai berhasil
                        _em_sl_ok = False
                        _em_sl_attempt = 0
                        _em_sl_last_notify = 0
                        while not _em_sl_ok:
                            _em_sl_attempt += 1
                            try:
                                algo_post_sl(symbol, _em_sl_side, _em_sl_price, quantity=_em_lot, position_side=_pos_side)
                                _em_sl_ok = True
                                send_telegram_raw(
                                    f"🛡️ <b>SL DARURAT DIPASANG — {symbol}</b>\n"
                                    f"{'─'*34}\n"
                                    f"Aktivasi posisi crash, posisi ditemukan naked.\n"
                                    f"{'🟢' if _em_dir == 'LONG' else '🔴'} {_em_dir} | Entry: {_em_entry}\n"
                                    f"🛑 SL darurat dipasang di: <b>{_em_sl_price}</b>"
                                    + (f" (setelah {_em_sl_attempt} percobaan)" if _em_sl_attempt > 1 else "") + "\n"
                                    f"⚠️ Periksa dan sesuaikan SL manual di Binance jika perlu!"
                                )
                            except Exception as _em_sl_err:
                                print(f"  ⚠️  [{symbol}] SL darurat attempt {_em_sl_attempt} gagal: {_em_sl_err}")
                                if _em_sl_attempt == 1 or (_em_sl_attempt - _em_sl_last_notify) >= 10:
                                    _em_sl_last_notify = _em_sl_attempt
                                    send_telegram_raw(
                                        f"⚠️ <b>SL DARURAT BELUM TERPASANG — {symbol}</b>\n"
                                        f"Percobaan ke-{_em_sl_attempt} gagal.\n"
                                        f"Error: <code>{_em_sl_err}</code>\n"
                                        f"🔁 Bot terus mencoba pasang SL darurat sampai berhasil "
                                        f"(posisi TIDAK akan di-close paksa)."
                                    )
                                _sync_server_time(force=True)
                                _em_sleep_s = 0.2 if _em_sl_attempt <= 5 else min(5.0, 0.5 * (_em_sl_attempt - 4))
                                time.sleep(_em_sleep_s)
                except Exception as _em_check_err:
                    print(f"  ⚠️  [{symbol}] Gagal cek posisi pasca-error aktivasi: {_em_check_err}")
                    send_telegram_raw(f"⚠️ <b>CEK POSISI GAGAL — {symbol}</b>\nVerifikasi manual apakah posisi terbuka tanpa SL!\nError: <code>{_em_check_err}</code>")

        elif status in ("CANCELED", "EXPIRED", "REJECTED"):
            print(f"  🗑  [{symbol}] Limit order {status} — hapus dari pending")
            # ── Bersihkan sisa open/conditional orders (jaga-jaga race condition) ──
            try:
                cancel_open_orders(symbol)
                print(f"  🗑  [{symbol}] Open/conditional orders di-cancel ({status} cleanup)")
            except Exception as _ce3:
                print(f"  ⚠️  [{symbol}] Gagal cancel open orders ({status}): {_ce3}")
            del pending_limit_orders[symbol]
            send_telegram_raw(
                f"🗑 <b>LIMIT ORDER {status} — {symbol}</b>\n"
                f"Order tidak aktif lagi + conditional orders dibersihkan.\n"
                f"Bot akan cari sinyal baru."
            )
        else:
            # NEW / PARTIALLY_FILLED — masih menunggu
            remaining_min = LIMIT_ORDER_TIMEOUT_MINUTES - elapsed_min
            print(f"  ⏳ [{symbol}] Limit order {status} | elapsed={elapsed_min:.0f}m | sisa={remaining_min:.0f}m")

            # ── FIX v38b: PARTIALLY_FILLED → SELALU cek positionRisk setiap scan ──
            # Bug sebelumnya (v34): hanya cek saat elapsed >= 80% timeout (400 menit)
            # Akibat: posisi sudah terbuka di Binance tapi SL/TP tidak dipasang
            # sampai 400 menit kemudian — atau tidak pernah kalau tidak sampai 400m.
            #
            # Fix: kalau status PARTIALLY_FILLED, LANGSUNG cek positionRisk.
            # Kalau posisi sudah terbuka → aktifkan + pasang SL/TP sekarang.
            # Kalau belum → lanjut menunggu (normal).
            if status == "PARTIALLY_FILLED":
                try:
                    _pf_positions = api_get("/fapi/v2/positionRisk", signed=True)
                    _pf_open = [
                        p for p in _pf_positions
                        if p["symbol"] == symbol and abs(float(p.get("positionAmt", 0))) > 0
                    ]
                    if _pf_open:
                        print(f"  ✅ [{symbol}] PARTIALLY_FILLED + posisi sudah terbuka → aktifkan + pasang SL/TP sekarang")
                        del pending_limit_orders[symbol]
                        send_telegram_raw(
                            f"⚠️ <b>PARTIALLY_FILLED AKTIF — {symbol}</b>\n"
                            f"{'─'*34}\n"
                            f"Order sebagian terisi dan posisi sudah terbuka.\n"
                            f"Bot mengaktifkan posisi dan memasang SL/TP otomatis."
                        )
                        try:
                            _activate_position_from_limit(symbol, pend)
                        except Exception as _pf_ae:
                            print(f"  ❌ [{symbol}] Error aktivasi PARTIALLY_FILLED: {_pf_ae}")
                            send_telegram_raw(f"❌ <b>ERROR AKTIVASI PARTIAL — {symbol}</b>\n<code>{_pf_ae}</code>")
                    else:
                        print(f"  ℹ️  [{symbol}] PARTIALLY_FILLED tapi belum ada posisi terbuka — tunggu fill lebih lanjut")
                except Exception as _pf_err:
                    print(f"  ⚠️  [{symbol}] Gagal cek positionRisk saat PARTIALLY_FILLED: {_pf_err}")




def get_open_orders(symbol: str) -> list:
    try:
        return api_get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
    except Exception:
        return []


def notify_tp1_hit(symbol: str, pos: dict, price: float):
    dir_em   = "🟢" if pos["direction"] == "LONG" else "🔴"
    tp1_pct  = int(TP1_PARTIAL * 100)
    tp2_pct  = 100 - tp1_pct
    entry    = pos["entry"]
    # PnL untuk porsi yang baru saja closed di TP1 (bukan seluruh posisi)
    _tp1_lot = pos.get("tp1_size", 0) or (pos.get("lot", 0) * TP1_PARTIAL)
    if pos["direction"] == "LONG":
        pnl_usdt = (price - entry) * _tp1_lot
    else:
        pnl_usdt = (entry - price) * _tp1_lot
    pnl_pct = ((price - entry) / entry * 100) if entry else 0
    if pos["direction"] == "SHORT":
        pnl_pct = -pnl_pct
    pnl_em = "🟢" if pnl_usdt >= 0 else "🔴"
    _tp2_size = pos.get("tp2_size", 0) or 0
    if _tp2_size > 0:
        msg = (
            f"🎯 <b>TP1 HIT — {symbol}</b>\n"
            f"{'─'*34}\n"
            f"{dir_em} Arah     : <b>{pos['direction']}</b>\n"
            f"💰 Entry   : <b>{entry}</b>\n"
            f"🎯 TP1     : <b>{pos['tp1']}</b>\n"
            f"📈 Harga   : <b>{price}</b>\n"
            f"{pnl_em} Profit ({tp1_pct}% lot): <b>{'+' if pnl_usdt>=0 else ''}{pnl_usdt:.2f} USDT "
            f"({'+' if pnl_pct>=0 else ''}{pnl_pct:.2f}%)</b>\n"
            f"✅ {tp1_pct}% lot CLOSED di TP1\n"
            f"🔄 SL sisa {tp2_pct}% lot digeser ke Break Even — trailing menuju TP2 ({pos['tp2']})\n"
        )
    else:
        msg = (
            f"🎯 <b>TAKE PROFIT HIT — {symbol}</b>\n"
            f"{'─'*34}\n"
            f"{dir_em} Arah     : <b>{pos['direction']}</b>\n"
            f"💰 Entry   : <b>{entry}</b>\n"
            f"🎯 Target  : <b>{pos['tp1']}</b>\n"
            f"📈 Harga   : <b>{price}</b>\n"
            f"{pnl_em} Profit   : <b>{'+' if pnl_usdt>=0 else ''}{pnl_usdt:.2f} USDT "
            f"({'+' if pnl_pct>=0 else ''}{pnl_pct:.2f}%)</b>\n"
            f"✅ Full posisi CLOSED (100%)\n"
            f"🎯 Target TP tercapai — trade selesai\n"
        )
    send_telegram_raw(msg)


def notify_sl_hit(symbol: str, pos: dict, price: float, sl_type: str = "Stop Loss"):
    entry    = pos["entry"]
    tp1_hit  = pos.get("tp1_hit", False)
    is_be    = sl_type == "Trailing Stop (Break Even)"
    is_manual = sl_type == "Manual Close (Profit)"
    pnl_pct  = ((price - entry) / entry * 100)
    if pos["direction"] == "SHORT":
        pnl_pct = -pnl_pct
    # BE atau manual profit = WIN
    is_win    = is_be or is_manual or pnl_pct > 0
    dir_em    = "🟢" if pos["direction"] == "LONG" else "🔴"
    # FIX Bug 3: emoji & header sesuai tipe close — bukan selalu 🛑 "SL HIT"
    if is_be:
        em         = "🔄"
        header_lbl = "TRAILING BE CLOSE"
    elif is_manual:
        em         = "✅"
        header_lbl = "MANUAL CLOSE (PROFIT)"
    else:
        em         = "🛑"
        header_lbl = "STOP LOSS HIT"
    color     = "⚪" if (is_be or is_manual) else "🔴"
    # FIX Bug 5: baca stats SETELAH update_performance sudah dipanggil oleh caller
    wins      = bot_state["wins"]
    losses    = bot_state["losses"]
    total     = wins + losses
    winrate   = (wins / total * 100) if total > 0 else 0
    result_em = "✅ WIN (BE)" if is_be else ("✅ WIN" if is_win else "❌ LOSS")
    is_real_sl = sl_type == "Stop Loss"
    if is_real_sl:
        cooldown_note = (
            f"⏳ <b>Cooldown {COOLDOWN_AFTER_SL_HOURS} jam aktif</b> — "
            f"bot tidak akan re-entry {symbol} selama {COOLDOWN_AFTER_SL_HOURS} jam."
        )
    elif is_be:
        cooldown_note = f"✅ Trailing BE — tidak ada cooldown, bot bisa re-entry {symbol} segera."
    else:
        cooldown_note = f"✅ Tidak ada cooldown — bot bisa re-entry {symbol} segera."
    # Keterangan lot yang di-close sekarang
    tp1_pct_lot = int(pos.get("tp1_size", 0) / max(pos.get("lot", 1), 1e-9) * 100) if tp1_hit else 0
    remaining_pct = 100 - tp1_pct_lot if tp1_hit else 100
    lot_note = (
        f"📦 Lot close: sisa {remaining_pct}% (TP1 sudah close {tp1_pct_lot}% sebelumnya)"
    ) if tp1_hit else (
        f"📦 Lot close: 100% (full posisi)"
    )
    if is_be:
        close_desc = "✅ Sisa posisi ditutup di Break Even — TP1 sudah ambil profit!"
    elif is_manual:
        close_desc = "✅ Posisi ditutup manual di area profit"
    elif is_win:
        close_desc = "✅ Posisi ditutup dengan profit"
    else:
        close_desc = "❌ Posisi ditutup dengan loss"
    msg = (
        f"{em} <b>{header_lbl} — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"{dir_em} Arah    : <b>{pos['direction']}</b>\n"
        f"💰 Entry  : <b>{entry}</b>\n"
        f"🛑 SL     : <b>{pos['sl']}</b>\n"
        f"📉 Harga  : <b>{price}</b>\n"
        f"{color} PnL sisa: <b>{'+'if pnl_pct>=0 else ''}{pnl_pct:.2f}%</b>\n"
        f"{lot_note}\n"
        f"{'─'*34}\n"
        f"{close_desc}\n"
        f"{'─'*34}\n"
        f"📊 <b>Hasil: {result_em}</b>\n"
        f"  ✅ Win : <b>{wins}</b>  |  ❌ Loss: <b>{losses}</b>  |  🎯 WR: <b>{winrate:.1f}%</b>\n"
        f"{'─'*34}\n"
        f"{cooldown_note}"
    )
    send_telegram_raw(msg)
    # ── Log trade ke file untuk analytics ────────────────────────────────
    try:
        _pnl_usdt = 0.0
        _entry    = pos.get("entry", 0)
        _close_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos.get("lot", 0)
        if not _close_lot:
            _close_lot = pos.get("lot", 0)
        if pos["direction"] == "LONG":
            _pnl_usdt = (price - _entry) * _close_lot
        else:
            _pnl_usdt = (_entry - price) * _close_lot
        _ct = "BE" if "Break Even" in sl_type else ("MANUAL" if "Manual" in sl_type else "SL")
        _rr_act = abs(price - _entry) / max(abs(_entry - pos.get("sl", _entry)), 1e-9)
        _btc_snap = _get_btc_btcd_snapshot()
        record_trade_close(
            symbol=symbol, direction=pos["direction"],
            entry=_entry, close_price=price,
            sl=pos.get("sl", 0), tp1=pos.get("tp1", 0), tp2=pos.get("tp2", 0),
            lot=_close_lot, pnl_usdt=_pnl_usdt,
            close_type=_ct,
            mode_label=pos.get("mode", {}).get("label", "?") if isinstance(pos.get("mode"), dict) else str(pos.get("mode", "?")),
            entry_tf=pos.get("entry_tf", "?"),
            score=pos.get("score", 0),
            rr_actual=round(_rr_act, 2),
            score_bd=pos.get("score_bd", {}),
            atr_pct=pos.get("atr_pct", 0),
            sl_atr_mult=pos.get("sl_atr_mult", 0),
            vol_rat=pos.get("vol_rat", 0),
            mkt_regime=pos.get("mkt_regime", "?"),
            alt_bull_pct=pos.get("alt_bull_pct", -1),
            alt_bear_pct=pos.get("alt_bear_pct", -1),
            htf_bias=pos.get("htf_bias", "?"),
            bos_strength=pos.get("bos_strength", "?"),
            engulf_break=pos.get("engulf_break", "?"),
            engulf_entry=pos.get("engulf_entry", "?"),
            engulf_note=pos.get("engulf_note", ""),
            fvg_retest=pos.get("fvg_retest", "?"),
            fvg_retest_note=pos.get("fvg_retest_note", ""),
            stoch_div=pos.get("stoch_div", "?"),
            stoch_weak=pos.get("stoch_weak", "?"),
            stoch_note=pos.get("stoch_note", ""),
            sweep_strength=pos.get("sweep_strength", "?"),
            sl_source=pos.get("sl_source", "?"),
            rsi_entry=pos.get("rsi_entry", 0),
            mae_pct=pos.get("mae_pct", 0),
            mfe_pct=pos.get("mfe_pct", 0),
            open_time=pos.get("open_time").isoformat() if isinstance(pos.get("open_time"), datetime) else str(pos.get("open_time", "")),
            btc_bias=pos.get("btc_bias", "?"),
            btcd_trend=pos.get("btcd_trend", "?"),
            btc_ltf_dir=pos.get("btc_ltf_dir", "?"),
            btcd_ltf_dir=pos.get("btcd_ltf_dir", "?"),
            btc_bias_at_close=_btc_snap["btc_bias_at_close"],
            btcd_trend_at_close=_btc_snap["btcd_trend_at_close"],
            btc_ltf_dir_at_close=_btc_snap["btc_ltf_dir_at_close"],
            btcd_ltf_dir_at_close=_btc_snap["btcd_ltf_dir_at_close"],
        )
    except Exception as _log_e:
        print(f"  ⚠️  record_trade_close (sl) error: {_log_e}")


def notify_tp2_hit(symbol: str, pos: dict, price: float):
    dir_em  = "🟢" if pos["direction"] == "LONG" else "🔴"
    entry   = pos["entry"]
    pnl_pct = ((price - entry) / entry * 100) if entry else 0
    if pos["direction"] == "SHORT":
        pnl_pct = -pnl_pct
    # PnL USDT untuk porsi yang closed di TP2 (sisa lot setelah TP1, atau full lot kalau TP1 belum kena)
    _tp2_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos.get("lot", 0)
    if not _tp2_lot:
        _tp2_lot = pos.get("lot", 0)
    if pos["direction"] == "LONG":
        pnl_usdt = (price - entry) * _tp2_lot
    else:
        pnl_usdt = (entry - price) * _tp2_lot
    # FIX Bug 5: baca stats SETELAH update_performance sudah dipanggil oleh caller
    wins    = bot_state["wins"]
    losses  = bot_state["losses"]
    total   = wins + losses
    winrate = (wins / total * 100) if total > 0 else 0
    tp1_hit = pos.get("tp1_hit", False)
    tp1_pct = int(TP1_PARTIAL * 100)
    tp2_pct_lot = 100 - tp1_pct
    if tp1_hit:
        lot_note = f"📦 Sisa {tp2_pct_lot}% lot CLOSED (TP1 {tp1_pct}% sudah kena sebelumnya)"
    else:
        lot_note = "📦 Full posisi CLOSED (100%) — TP2 kena langsung tanpa TP1 tersentuh"
    msg = (
        f"🏆 <b>TAKE PROFIT HIT — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"{dir_em} Arah    : <b>{pos['direction']}</b>\n"
        f"💰 Entry  : <b>{entry}</b>\n"
        f"🎯 Target  : <b>{pos['tp2']}</b>\n"
        f"📈 Harga  : <b>{price}</b>\n"
        f"📊 PnL    : <b>{'+' if pnl_usdt>=0 else ''}{pnl_usdt:.2f} USDT "
        f"({'+' if pnl_pct>=0 else ''}{pnl_pct:.2f}%)</b>\n"
        f"{lot_note}\n"
        f"{'─'*34}\n"
        f"✅ <b>Trade Selesai!</b>\n"
        f"{'─'*34}\n"
        f"📊 <b>Hasil: ✅ WIN</b>\n"
        f"  ✅ Win : <b>{wins}</b>  |  ❌ Loss: <b>{losses}</b>  |  🎯 WR: <b>{winrate:.1f}%</b>"
    )
    send_telegram_raw(msg)
    # ── Log trade ke file untuk analytics ────────────────────────────────
    try:
        _entry    = pos.get("entry", 0)
        _close_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos.get("lot", 0)
        if not _close_lot:
            _close_lot = pos.get("lot", 0)
        if pos["direction"] == "LONG":
            _pnl_usdt = (price - _entry) * _close_lot
        else:
            _pnl_usdt = (_entry - price) * _close_lot
        _rr_act = abs(price - _entry) / max(abs(_entry - pos.get("sl", _entry)), 1e-9)
        _btc_snap = _get_btc_btcd_snapshot()
        record_trade_close(
            symbol=symbol, direction=pos["direction"],
            entry=_entry, close_price=price,
            sl=pos.get("sl", 0), tp1=pos.get("tp1", 0), tp2=pos.get("tp2", 0),
            lot=_close_lot, pnl_usdt=_pnl_usdt,
            close_type="TP2",
            mode_label=pos.get("mode", {}).get("label", "?") if isinstance(pos.get("mode"), dict) else str(pos.get("mode", "?")),
            entry_tf=pos.get("entry_tf", "?"),
            score=pos.get("score", 0),
            rr_actual=round(_rr_act, 2),
            score_bd=pos.get("score_bd", {}),
            atr_pct=pos.get("atr_pct", 0),
            sl_atr_mult=pos.get("sl_atr_mult", 0),
            vol_rat=pos.get("vol_rat", 0),
            mkt_regime=pos.get("mkt_regime", "?"),
            alt_bull_pct=pos.get("alt_bull_pct", -1),
            alt_bear_pct=pos.get("alt_bear_pct", -1),
            htf_bias=pos.get("htf_bias", "?"),
            bos_strength=pos.get("bos_strength", "?"),
            engulf_break=pos.get("engulf_break", "?"),
            engulf_entry=pos.get("engulf_entry", "?"),
            engulf_note=pos.get("engulf_note", ""),
            fvg_retest=pos.get("fvg_retest", "?"),
            fvg_retest_note=pos.get("fvg_retest_note", ""),
            stoch_div=pos.get("stoch_div", "?"),
            stoch_weak=pos.get("stoch_weak", "?"),
            stoch_note=pos.get("stoch_note", ""),
            sweep_strength=pos.get("sweep_strength", "?"),
            sl_source=pos.get("sl_source", "?"),
            rsi_entry=pos.get("rsi_entry", 0),
            mae_pct=pos.get("mae_pct", 0),
            mfe_pct=pos.get("mfe_pct", 0),
            open_time=pos.get("open_time").isoformat() if isinstance(pos.get("open_time"), datetime) else str(pos.get("open_time", "")),
            btc_bias=pos.get("btc_bias", "?"),
            btcd_trend=pos.get("btcd_trend", "?"),
            btc_ltf_dir=pos.get("btc_ltf_dir", "?"),
            btcd_ltf_dir=pos.get("btcd_ltf_dir", "?"),
            btc_bias_at_close=_btc_snap["btc_bias_at_close"],
            btcd_trend_at_close=_btc_snap["btcd_trend_at_close"],
            btc_ltf_dir_at_close=_btc_snap["btc_ltf_dir_at_close"],
            btcd_ltf_dir_at_close=_btc_snap["btcd_ltf_dir_at_close"],
        )
    except Exception as _log_e:
        print(f"  ⚠️  record_trade_close (tp2) error: {_log_e}")


def notify_trailing_activated(symbol: str, pos: dict, new_sl: float):
    msg = (
        f"🔄 <b>TRAILING STOP AKTIF — {symbol}</b>\n"
        f"{'─'*34}\n"
        f"💰 Entry       : <b>{pos['entry']}</b>\n"
        f"🛑 SL Lama     : <b>{pos['sl']}</b>\n"
        f"✅ SL Baru     : <b>{new_sl}</b> (Break Even)\n"
        f"{'─'*34}\n"
        f"🔒 Posisi terlindungi dari loss"
    )
    send_telegram_raw(msg)


_structure_close_in_progress: set = set()   # symbol yang lagi diproses force-close
                                              # dipakai untuk cegah TP1 self-heal race

def _get_btc_btcd_snapshot() -> dict:
    """
    Ambil snapshot kondisi BTC + BTCD SAAT INI (real-time), untuk dicatat di
    trade_log.json pada momen close (SL/BE/manual) — terpisah dari
    btc_bias/btcd_trend versi entry yang sudah ada di record pos.
    Tujuan: bisa evaluasi post-hoc apakah SL disebabkan/didahului oleh
    BTC atau BTCD berbalik arah selagi posisi berjalan, bukan cuma tahu
    kondisi makro saat entry dibuka.
    Reuse fetch_btcd_ohlcv/fetch_btc_ltf_data yang sudah di-cache (TTL~3menit)
    jadi aman dipanggil di setiap close tanpa menambah beban API signifikan.
    Return dict, semua field default "?" kalau fetch gagal (never raises).
    """
    snap = {
        "btc_bias_at_close":     "?",
        "btcd_trend_at_close":   "?",
        "btc_ltf_dir_at_close":  "?",
        "btcd_ltf_dir_at_close": "?",
    }
    try:
        snap["btc_bias_at_close"] = get_btc_bias()
    except Exception as e:
        print(f"  ⚠️  _get_btc_btcd_snapshot btc_bias error: {e}")
    try:
        snap["btcd_trend_at_close"] = get_btcd_bias()
    except Exception as e:
        print(f"  ⚠️  _get_btc_btcd_snapshot btcd_trend error: {e}")
    try:
        _df_btc_ltf = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
        snap["btc_ltf_dir_at_close"] = get_btc_ltf_direction(_df_btc_ltf)
    except Exception as e:
        print(f"  ⚠️  _get_btc_btcd_snapshot btc_ltf_dir error: {e}")
    try:
        _df_btcd_ltf = fetch_btcd_ohlcv_isolated(tf=BTCDOM_LTF_TF, limit=80)
        snap["btcd_ltf_dir_at_close"] = get_btcd_ltf_direction(_df_btcd_ltf)
    except Exception as e:
        print(f"  ⚠️  _get_btc_btcd_snapshot btcd_ltf_dir error: {e}")
    return snap




# ═══════════════════════════════════════════════════════════════════════════
# SL BACKSTOP (v82) — pengaman terakhir kalau order SL di Binance hilang/gagal
# ═══════════════════════════════════════════════════════════════════════════
# Kasus nyata SANDUSDT SHORT: SL=0.0675, entry=0.0657, tapi harga lari ke
# 0.0764 (+16%) selama 42 jam dan posisi tidak pernah ditutup (-178 USDT,
# padahal batas risiko MAX_SL_LOSS_PCT cuma 1% balance). Sebelumnya bot
# 100% bergantung pada order SL di exchange; kalau order itu CANCELED/EXPIRED/
# tidak pernah ada, tidak ada lapisan lain yang menutup posisi.
SL_BACKSTOP_TOLERANCE = 0.002   # 0.2% lewat dari SL → anggap order exchange gagal
_sl_backstop_last_try: dict = {}


def _sl_backstop_check(symbol: str, pos: dict, price: float) -> bool:
    """Return True kalau backstop berhasil menutup posisi (caller skip iterasi)."""
    try:
        sl, direction = pos.get("sl"), pos.get("direction")
        if not sl or sl <= 0 or not price:
            return False
        if direction == "LONG":
            breached = price <= sl * (1 - SL_BACKSTOP_TOLERANCE)
        else:
            breached = price >= sl * (1 + SL_BACKSTOP_TOLERANCE)
        if not breached:
            return False

        # Kalau order SL asli masih hidup / sudah FILLED, biarkan exchange +
        # jalur close-detection yang urus (beri waktu 1 siklus polling).
        _ref = pos.get("sl_order_ref")
        _st  = check_order_status_direct(symbol, _ref) if _ref else None
        if _st in ("FILLED", "NEW"):
            # NEW padahal harga sudah lewat SL+tol = order macet; tetap backstop
            # setelah 3 siklus (~24 dtk) supaya tidak race dengan fill normal.
            if _st == "FILLED":
                return False
            _first = _sl_backstop_last_try.setdefault(symbol + ":seen", time.time())
            if time.time() - _first < TRAILING_POLL_INTERVAL * 3:
                return False

        # Throttle percobaan close (maks 1x / 10 dtk per symbol)
        if time.time() - _sl_backstop_last_try.get(symbol, 0) < 10:
            return False
        _sl_backstop_last_try[symbol] = time.time()

        pside = pos.get("position_side", "BOTH")
        qty = 0.0
        for p in api_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True):
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            if pside != "BOTH" and p.get("positionSide") != pside:
                continue
            qty = abs(amt)
            break
        if qty <= 0:
            return False

        close_side = "BUY" if direction == "SHORT" else "SELL"
        _f = get_lot_filters(symbol)
        _mx = _f.get("mkt_maxQty", _f.get("maxQty", 9e12)) or 9e12
        _step = _f.get("mkt_stepSize", _f.get("stepSize", 0.001)) or 0.001
        _prec = _f.get("quantityPrecision")
        if _prec is None:
            _prec = max(0, round(-math.log10(_step)))
        _sync_server_time(force=True)
        remaining = qty
        while remaining > 0:
            chunk = min(remaining, _mx)
            params = {"symbol": symbol, "side": close_side, "type": "MARKET",
                      "quantity": format_qty(chunk, _step, _prec)}
            if pside != "BOTH":
                params["positionSide"] = pside
            else:
                params["reduceOnly"] = "true"
            api_post("/fapi/v1/order", params)
            remaining = round(remaining - chunk, 10)
        _sl_backstop_last_try.pop(symbol + ":seen", None)
        send_telegram_raw(
            f"🛡️ <b>SL BACKSTOP — {symbol} {direction}</b>\n"
            f"Harga {price} sudah melewati SL {sl} tapi posisi masih terbuka "
            f"(order SL status: {_st or 'tidak ada/hilang'}).\n"
            f"Posisi ditutup MARKET oleh bot. Cek log kenapa order SL tidak jalan."
        )
        return True
    except Exception as e:
        print(f"  ⚠️  [{symbol}] _sl_backstop_check error: {e}")
        try:
            send_telegram_raw(f"🆘 <b>SL BACKSTOP GAGAL — {symbol}</b>\nTutup manual di Binance!\n<code>{e}</code>")
        except Exception:
            pass
        return False


_sl_heal_last: dict = {}
SL_HEAL_INTERVAL = 60   # detik antar pengecekan order SL per symbol


def _sl_selfheal_check(symbol: str, pos: dict, price: float) -> None:
    """v82: kalau order SL asli di Binance CANCELED/EXPIRED/REJECTED (atau tidak
    pernah tercatat), pasang ulang SL pada level pos['sl'].
    Hanya untuk fase SEBELUM TP1 (SL masih SL awal). Fase setelah TP1/trailing
    ditangani cabang 'unprotected' yang sudah ada."""
    try:
        if pos.get("tp1_hit") or pos.get("trailing_active") or pos.get("unprotected"):
            return
        if time.time() - _sl_heal_last.get(symbol, 0) < SL_HEAL_INTERVAL:
            return
        _sl_heal_last[symbol] = time.time()

        ref = pos.get("sl_order_ref")
        st = check_order_status_direct(symbol, ref) if ref else "MISSING"
        if st not in ("CANCELED", "EXPIRED", "REJECTED", "MISSING"):
            return   # NEW / FILLED / None(network) → jangan sentuh

        sl, direction = pos.get("sl"), pos.get("direction")
        if not sl or sl <= 0:
            return
        # harga sudah lewat SL → backstop yang urus (dipanggil sebelum ini)
        if (direction == "SHORT" and price >= sl) or (direction == "LONG" and price <= sl):
            return

        pside = pos.get("position_side", "BOTH")
        qty = 0.0
        for pr in api_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True):
            amt = float(pr.get("positionAmt", 0))
            if amt == 0 or (pside != "BOTH" and pr.get("positionSide") != pside):
                continue
            qty = abs(amt)
            break
        if qty <= 0:
            return

        sl_side = "BUY" if direction == "SHORT" else "SELL"
        with _order_mgmt_lock:
            new_ref = algo_post_sl(symbol, sl_side, sl, quantity=qty, position_side=pside)
        with _positions_lock:
            if symbol in active_positions:
                active_positions[symbol]["sl_order_ref"] = new_ref
        save_state()
        send_telegram_raw(
            f"🔧 <b>SL DIPASANG ULANG — {symbol} {direction}</b>\n"
            f"Order SL lama berstatus <b>{st}</b> (hilang dari Binance).\n"
            f"SL baru @ {sl}, qty {qty}."
        )
    except Exception as e:
        print(f"  ⚠️  [{symbol}] _sl_selfheal_check error: {e}")


def manage_trailing():
    # ── FIX BUG: bersihkan entry _recent_close_guard yang sudah kedaluwarsa ──
    # Kecil, tapi mencegah dict ini tumbuh tak terbatas seiring waktu bot jalan.
    if _recent_close_guard:
        _now_guard = time.time()
        for _sym in [s for s, ts in _recent_close_guard.items()
                     if (_now_guard - ts) >= DUPLICATE_CLOSE_GUARD_SECONDS]:
            _recent_close_guard.pop(_sym, None)

    for symbol, pos in list(active_positions.items()):
        try:
            # FIX: ambil snapshot pos dengan lock agar tidak race condition dengan sync_closed_positions
            with _positions_lock:
                if symbol not in active_positions:
                    continue
                pos = dict(active_positions[symbol])  # snapshot thread-safe
            price = get_current_price(symbol)
            if price is None:
                continue

            entry     = pos["entry"]
            sl        = pos["sl"]
            tp1       = pos["tp1"]
            tp2       = pos["tp2"]
            direction = pos["direction"]


            # ── MAE/MFE water mark — untuk analisis trade log ────────────────
            # MAE (max adverse excursion) = seberapa jauh harga sempat lawan
            # arah posisi sebelum akhirnya close. MFE (max favorable) =
            # seberapa jauh sempat searah profit. Dipakai untuk bedain
            # "SL kena karena struktural salah dari awal" vs "SL kena padahal
            # sempat mendekati/menyentuh area profit dulu (whipsaw/noise)".
            if entry:
                if direction == "LONG":
                    _adverse_pct   = (entry - price) / entry * 100
                    _favorable_pct = (price - entry) / entry * 100
                else:
                    _adverse_pct   = (price - entry) / entry * 100
                    _favorable_pct = (entry - price) / entry * 100
                _new_mae = max(pos.get("mae_pct", 0.0), _adverse_pct)
                _new_mfe = max(pos.get("mfe_pct", 0.0), _favorable_pct)
                if _new_mae != pos.get("mae_pct", 0.0) or _new_mfe != pos.get("mfe_pct", 0.0):
                    with _positions_lock:
                        if symbol in active_positions:
                            active_positions[symbol]["mae_pct"] = round(_new_mae, 4)
                            active_positions[symbol]["mfe_pct"] = round(_new_mfe, 4)

            # ── Re-fetch tick_size dari filters terbaru ──────────────────────
            # Jangan pakai pos["tick_size"] yang tersimpan saat entry — bisa stale.
            # tick_size yang salah → format_price salah → -1111 di algoOrder.
            _live_filters = get_lot_filters(symbol)
            tick_size = _live_filters.get("tickSize", pos.get("tick_size", 0.01))
            step_size = _live_filters.get("stepSize", pos.get("step_size", 0.001))

            sl_side   = "SELL" if pos["side"] == "BUY" else "BUY"

            still_open = has_position(symbol)
            if not still_open:
                # FIX Bug 1: Guard double notif — jika sync_closed_positions sudah hapus posisi, skip
                if symbol not in active_positions:
                    continue

                # ── FIX BUG: duplicate-close guard ────────────────────────────────
                # Kalau symbol ini BARU SAJA di-commit closed (lihat _mark_position_closed
                # di bawah), has_position()==False sekali baca TIDAK CUKUP untuk
                # dipercaya — verifikasi ulang beberapa kali ke Binance sebelum commit
                # close KEDUA untuk symbol yang sama. Ini mencegah kasus nyata
                # (NEARUSDT ter-log SL 18× dalam 33 menit padahal entry & lot identik
                # → satu posisi yang sama, bukan 18 kekalahan beruntun) terulang.
                if _is_recently_closed(symbol):
                    if not confirm_position_really_closed(symbol):
                        # Posisi ternyata masih open di Binance — batalkan proses close
                        # kali ini, biarkan loop berikutnya cek ulang dari awal.
                        continue
                    print(f"  🛡️  [{symbol}] Duplicate-close guard: terverifikasi benar-benar "
                          f"closed (bukan re-deteksi palsu) — lanjut proses close.")

                # ── FIX DOUBLE-LOG (TP2 + BE untuk 1 trade): CLAIM ATOMIK ──────────
                # manage_trailing() (thread trailing) dan sync_closed_positions()
                # (main loop) bisa mendeteksi close yang SAMA hampir bersamaan.
                # Cek `symbol not in active_positions` di atas tidak atomik dengan
                # pop di bawah, jadi dua-duanya lolos lalu masing2 memanggil
                # update_performance + record_trade_close: satu dilabel TP2,
                # satunya BE. Sekarang siapa yang berhasil pop() duluan = pemilik
                # tunggal proses close; yang kalah langsung skip.
                with _positions_lock:
                    _claimed_pos = active_positions.pop(symbol, None)
                if _claimed_pos is None:
                    print(f"  🛡️  [{symbol}] Close sudah diproses thread lain — skip (anti double-log).")
                    continue

                # ── FIX KRITIS: bersihkan sisa SL/TP order SEGERA saat close terdeteksi ──
                # Sebelumnya, jalur close-detection INI (manage_trailing / monitor loop
                # real-time) TIDAK PERNAH memanggil cancel_open_orders() — beda dengan
                # sync_closed_positions() yang sudah benar. Akibatnya: kalau posisi close
                # kedeteksi lewat jalur ini duluan (lebih sering terjadi karena loop-nya
                # jalan tiap scan, bukan periodic sync), order SL/TP yang TIDAK ikut
                # kena (misal TP2 hit duluan → SL order lama masih nyangkut di Binance)
                # dibiarkan begitu saja sampai auto-cleanup orphan jalan (bisa sampai
                # 1 jam kemudian). Sekarang dibersihkan LANGSUNG di sini juga.
                try:
                    cancel_open_orders(symbol)
                    print(f"  🧹 [{symbol}] Sisa SL/TP order dibersihkan segera setelah close terdeteksi.")
                except Exception as _cancel_err:
                    print(f"  ⚠️  [{symbol}] Gagal cancel sisa order setelah close: {_cancel_err}")

                # ── FIX KRITIS: pakai harga FILL ASLI dari Binance, bukan mark
                # price live. `price` (dari get_current_price di atas) adalah
                # harga PASAR SAAT INI — kalau posisi close karena SL slip di
                # harga jauh lebih rendah dan pasar sempat rebound sebelum bot
                # sempat deteksi, `price` di sini SUDAH TIDAK MEWAKILI harga
                # exit yang sebenarnya. Ini yang menyebabkan notif salah total
                # (melaporkan "profit" padahal SL kena dengan slippage — kasus
                # nyata: SL=82.5338, fill asli ~81.5-81.6, tapi mark price saat
                # bot cek sudah rebound ke 83.38 → notif keliru bilang untung).
                _actual_close_price = get_actual_close_price(symbol)
                if _actual_close_price is not None:
                    price = _actual_close_price
                else:
                    print(f"  ⚠️  [{symbol}] Gagal ambil harga fill asli — fallback ke mark price "
                          f"(PnL/label di notif berikut bisa kurang akurat).")

                # Tentukan apakah SL atau TP yang kena
                # PENTING: price dari monitoring loop bisa stale (sudah bergerak setelah fill).
                # Cek urutan: TP2 dulu (profit tertinggi), lalu TP1 trail, lalu SL, lalu fallback.
                _is_sl = False
                _notify_type = None  # "tp2" | "be" | "sl" | "manual"

                # ── FIX: sebelum jatuh ke tebakan harga (yang bisa salah label jadi
                # "manual" padahal TP1/TP2 beneran fill di Binance tapi tp1_hit di
                # memory belum sempat ke-update oleh iterasi loop sebelumnya — race
                # antara has_position() jadi False duluan vs verifikasi order status
                # yang seharusnya jalan di blok bawah), cek dulu status order ASLI
                # ke Binance lewat ID yang tersimpan. Ini sumber kebenaran yang sama
                # dipakai verifikasi tp1_hit di bawah — jangan tebak dari harga saja.
                _tp1_ref_chk = pos.get("tp1_order_ref")
                _tp2_ref_chk = pos.get("tp2_order_ref")
                _sl_ref_chk  = pos.get("sl_order_ref")
                _tp1_status_chk = check_order_status_direct(symbol, _tp1_ref_chk) if _tp1_ref_chk else None
                _tp2_status_chk = check_order_status_direct(symbol, _tp2_ref_chk) if _tp2_ref_chk else None
                _sl_status_chk  = check_order_status_direct(symbol, _sl_ref_chk)  if _sl_ref_chk  else None

                if _tp2_status_chk == "FILLED" or _tp1_status_chk == "FILLED":
                    _notify_type = "tp2"
                elif _sl_status_chk == "FILLED":
                    _is_sl       = True
                    _notify_type = "sl"
                elif _tp1_status_chk == "FILLED" and not pos.get("tp1_hit"):
                    # TP1 order asli benar-benar fill di Binance tapi tp1_hit di memory
                    # belum sempat ter-update — treat sisa lot sebagai BE/trailing close,
                    # bukan "manual".
                    _notify_type = "be"
                elif direction == "LONG":
                    if price >= tp2:
                        _notify_type = "tp2"
                    elif pos.get("tp1_hit") and price >= entry:
                        # TP1 sudah kena + harga masih di atas entry → trailing BE close
                        _notify_type = "be"
                    elif price <= sl or price < entry:
                        # FIX Bug 2: "trailing_active" bukan penentu label SL
                        # Label "Trailing BE" hanya valid jika tp1_hit=True DAN harga di area entry
                        # Kalau harga kena SL murni (turun ke bawah SL/entry) → Stop Loss biasa
                        _is_sl       = True
                        _notify_type = "sl"
                    else:
                        # Tidak ada order TP/SL yang terkonfirmasi fill dan harga di
                        # zona ambigu — kemungkinan order status gagal dicek (network)
                        # atau memang benar close di luar bot. Label sebagai "unknown"
                        # (bukan otomatis "manual") supaya tidak menuduh keliru.
                        _is_profit   = price > entry
                        _is_sl       = not _is_profit
                        _notify_type = "manual" if _is_profit else "sl"
                else:  # SHORT
                    if price <= tp2:
                        _notify_type = "tp2"
                    elif pos.get("tp1_hit") and price <= entry:
                        _notify_type = "be"
                    elif price >= sl or price > entry:
                        # FIX Bug 2: sama — label SL murni, bukan trailing jika harga kena SL
                        _is_sl       = True
                        _notify_type = "sl"
                    else:
                        _is_profit   = price < entry
                        _is_sl       = not _is_profit
                        _notify_type = "manual" if _is_profit else "sl"

                # FIX: kalau baru terkonfirmasi TP1 FILLED lewat cek order status di
                # atas (race — tp1_hit di memory belum sempat ke-update), anggap
                # tp1_hit True di sini juga supaya _close_lot & _is_be konsisten.
                if _tp1_status_chk == "FILLED" and not pos.get("tp1_hit"):
                    pos["tp1_hit"] = True

                # Kalau TP1 sudah kena + posisi close di entry (BE) → dihitung WIN
                _is_be  = pos.get("tp1_hit", False) and not _is_sl
                _is_win = _is_be or \
                          (direction == "LONG" and price > entry) or \
                          (direction == "SHORT" and price < entry)
                # Kalau TP1 sudah kena sebelumnya, lot yang tersisa hanya tp2_size.
                _close_lot = pos.get("tp2_size") if pos.get("tp1_hit") else pos.get("lot", 0)
                if not _close_lot:
                    _close_lot = pos.get("lot", 0)
                if direction == "LONG":
                    _real_pnl = (price - entry) * _close_lot
                else:
                    _real_pnl = (entry - price) * _close_lot
                # BE close: anggap PnL sedikit positif agar update_performance hitung WIN bukan LOSS
                if _is_be and _real_pnl <= 0:
                    _real_pnl = 0.001
                # FIX Bug 5: update_performance SEBELUM notify agar stats di notif sudah terupdate
                update_performance(_real_pnl)
                with _positions_lock:
                    active_positions.pop(symbol, None)
                _mark_position_closed(symbol)   # FIX BUG: duplicate-close guard — catat waktu commit
                save_state()   # FIX: sinkron ke disk begitu posisi close (TP/SL/manual)
                set_cooldown_post_close(
                    symbol,
                    reason="posisi close terdeteksi trailing monitor",
                    is_stoploss=_is_sl
                )
                if _is_sl:
                    record_pair_sl(symbol)
                # FIX Bug 1 & 3: kirim notif SETELAH update_performance & hapus posisi
                # — stats sudah fresh, tidak ada double-notif dari sync_closed_positions
                if _notify_type == "tp2":
                    notify_tp2_hit(symbol, pos, price)
                elif _notify_type == "be":
                    notify_sl_hit(symbol, pos, price, "Trailing Stop (Break Even)")
                elif _notify_type == "manual":
                    notify_sl_hit(symbol, pos, price, "Manual Close (Profit)")
                else:
                    notify_sl_hit(symbol, pos, price, "Stop Loss")
                continue

            # ── v82: SL BACKSTOP — posisi masih open tapi harga sudah lewat SL ──
            if _sl_backstop_check(symbol, pos, price):
                continue
            _sl_selfheal_check(symbol, pos, price)

            if not pos.get("tp1_hit"):
                # ── FIX KRITIS — cek status order LANGSUNG ke Binance ────────────
                # Pendekatan lama menebak "apakah TP1 sudah kena" dari kombinasi
                # price-cross + lot tersisa di positionRisk. Ini terbukti bisa salah
                # simpul TP1 "sudah kena" padahal harga belum pernah menyentuh level
                # TP1 sama sekali (kasus nyata: SHORT TP1=62933, notif TP1 HIT
                # muncul di harga 63079 — masih jauh dari TP1).
                #
                # FIX: sumber kebenaran sekarang adalah STATUS ORDER TP1 ASLI itu
                # sendiri (algoId yang disimpan saat order dipasang), dicek langsung
                # via GET /fapi/v1/algoOrder. Kalau algoStatus-nya FINISHED/TRIGGERED
                # → order itu benar-benar sudah dieksekusi Binance, titik. Tidak ada
                # lagi tebak-tebakan dari harga atau estimasi lot.
                tp1_hit = False
                _tp1_ref = pos.get("tp1_order_ref")
                if _tp1_ref:
                    _tp1_status = check_order_status_direct(symbol, _tp1_ref)
                    if _tp1_status == "FILLED":
                        tp1_hit = True
                    elif _tp1_status in ("CANCELED",):
                        # ── FIX: self-heal ghost-TP1 — order TP1 asli ternyata sudah
                        # dibatalkan/expired di Binance tanpa sempat fill (misal ke-cancel
                        # manual, kena orphan cleanup, atau race dengan cancel_open_orders()
                        # dari proses lain). Sebelumnya kondisi ini cuma di-log, TIDAK ada
                        # yang benar-benar pasang ulang TP1 → posisi bisa jalan tanpa TP1
                        # selamanya sampai kena SL. Sekarang: pasang ulang otomatis.
                        # FIX RACE: jangan pasang ulang TP1 kalau symbol ini lagi
                        # diproses structure-reversal close — posisi mau ditutup,
                        # bukan dilanjut. Sebelumnya heal ini bisa re-arm TP1 tepat
                        # setelah cancel_open_orders() di close function, bikin loop
                        # STRUCTURE CLOSE GAGAL ↔ TP1 DIPASANG ULANG berulang-ulang.
                        if symbol in _structure_close_in_progress:
                            print(f"  ⏸️  [{symbol}] Skip TP1 self-heal — structure close sedang berjalan.")
                        else:
                            print(f"  ⚠️  [{symbol}] Order TP1 asli berstatus CANCELED/EXPIRED "
                                  f"di Binance (bukan FILLED) — posisi kehilangan TP1, pasang ulang...")
                            try:
                                # FIX RACE: pegang lock yang sama dipakai cleanup_duplicate_
                                # position_orders(), supaya cleanup tidak jalan di tengah-tengah
                                # proses pasang-ulang TP1 ini (mencegah TP1 baru langsung
                                # ke-cancel lagi karena dianggap "duplikat").
                                with _order_mgmt_lock:
                                    _pos_side_heal = pos.get("position_side", "BOTH")
                                    _tp1_size_heal = pos.get("tp1_size", 0)
                                    _min_qty_heal  = pos.get("min_qty", 0.001)
                                    if _tp1_size_heal >= _min_qty_heal:
                                        _fresh_heal = get_current_price(symbol) or price
                                        _tp1_price_heal = adjust_tp_price(tp1, _fresh_heal, direction, tick_size, buffer_pct=0.002)
                                        _new_tp1_ref = algo_post_tp(symbol, sl_side, _tp1_price_heal, _tp1_size_heal, position_side=_pos_side_heal)
                                        with _positions_lock:
                                            if symbol in active_positions:
                                                active_positions[symbol]["tp1_order_ref"] = _new_tp1_ref
                                        save_state()
                                    send_telegram_raw(
                                        f"🔧 <b>TP1 DIPASANG ULANG — {symbol}</b>\n"
                                        f"Order TP1 asli ke-cancel di Binance tanpa fill. Sudah dipasang ulang otomatis."
                                    )
                                    print(f"  ✅ [{symbol}] TP1 berhasil dipasang ulang setelah CANCELED/EXPIRED.")
                            except Exception as _heal_e:
                                print(f"  ⚠️  [{symbol}] Gagal pasang ulang TP1 setelah CANCELED: {_heal_e}")
                                send_telegram_raw(
                                    f"🚨 <b>TP1 HILANG & GAGAL DIPASANG ULANG — {symbol}</b>\n"
                                    f"Order TP1 ke-cancel di Binance dan bot gagal pasang ulang.\n"
                                    f"Error: <code>{_heal_e}</code>\nCek dan pasang manual jika perlu!"
                                )
                    # status "NEW" atau None (gagal cek) → tp1_hit tetap False, coba lagi nanti
                else:
                    # Posisi tanpa referensi order tersimpan (misal posisi pre-existing
                    # yang di-import sebelum fix ini, atau order sempat displit tanpa
                    # ref tersimpan) — fallback ke cek positionRisk sebagai pengganti,
                    # TAPI tetap wajib price-cross asli supaya tidak salah simpul.
                    _price_crossed = (direction == "LONG" and price >= tp1) or \
                                      (direction == "SHORT" and price <= tp1)
                    if _price_crossed:
                        try:
                            _risk = api_get("/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
                            _pos_side_chk = pos.get("position_side", "BOTH")
                            _real_amt = 0.0
                            _found_match = False
                            for _rp in _risk:
                                if _rp.get("symbol") != symbol:
                                    continue
                                if _pos_side_chk in ("LONG", "SHORT") and _rp.get("positionSide") != _pos_side_chk:
                                    continue
                                _real_amt = abs(float(_rp.get("positionAmt", 0)))
                                _found_match = True
                                break
                            _full_lot = pos.get("lot", 0)
                            _expected_remaining = pos.get("tp2_size") or max(_full_lot - pos.get("tp1_size", 0), 0)
                            _tol = max(pos.get("tp1_size", 0) * 0.2, pos.get("min_qty", 0) or 0, 1e-9)
                            if _found_match and _real_amt > 0 and _real_amt <= (_expected_remaining + _tol):
                                tp1_hit = True
                        except Exception as _tp1_chk_err:
                            print(f"  ⚠️  [{symbol}] Gagal verifikasi tp1_hit (fallback tanpa order_ref): {_tp1_chk_err}")

                if tp1_hit:
                    # FIX race condition: guard write dengan lock + cek symbol masih ada
                    with _positions_lock:
                        if symbol not in active_positions:
                            continue
                        active_positions[symbol]["tp1_hit"]    = True
                        active_positions[symbol]["tp1_hit_at"] = datetime.now(timezone.utc).isoformat()
                        if "original_sl" not in active_positions[symbol]:
                            active_positions[symbol]["original_sl"] = sl
                    save_state()   # FIX: simpan progress tp1_hit ke disk agar tidak hilang saat restart
                    # FIX BUG #4: pakai live data bukan snapshot stale dari loop
                    notify_tp1_hit(symbol, active_positions[symbol], price)
                    new_sl = adjust_sl_price(be_target_price(entry, direction), price, direction, tick_size, buffer_pct=0.005)
                    try:
                        # ── FIX: sebelumnya cancel_open_orders(symbol) di sini membatalkan
                        # SEMUA order simbol ini (SL DAN TP2 sekaligus), padahal level TP2
                        # sama sekali tidak berubah saat TP1 hit — cuma SL yang perlu diganti
                        # ke breakeven. Itu bikin ada jeda ("closing window") antara TP2 lama
                        # dibatalkan sampai TP2 baru dipasang ulang di bawah; kalau di jeda itu
                        # ada gangguan network/API, TP2 bisa gagal terpasang ulang sementara
                        # yang lama udah kadung hilang → posisi sempat jalan TANPA TP2 walau
                        # posisinya masih hidup.
                        #
                        # FIX: sekarang cuma cancel order SL (cancel_sl_orders_only) — TP1/TP2
                        # tidak pernah disentuh sama sekali di titik ini, jadi order TP2 asli
                        # (beserta tp2_order_ref-nya) tetap hidup terus tanpa jeda apapun.
                        cancel_sl_orders_only(symbol)
                        time.sleep(0.3)
                        _remaining_lot = pos.get("tp2_size") or pos.get("lot", 0)
                        _pos_side = pos.get("position_side", "BOTH")
                        sl_placed = False
                        # FIX: retry lebih banyak dengan delay naik — SL BE kritis, jangan cepat menyerah
                        for _attempt, _buf in enumerate((0.000, 0.005, 0.010, 0.015, 0.020)):
                            try:
                                fresh = get_current_price(symbol) or price
                                new_sl = adjust_sl_price(be_target_price(entry, direction), fresh, direction, tick_size, buffer_pct=max(_buf, 0.005))
                                algo_post_sl(symbol, sl_side, new_sl, quantity=_remaining_lot, position_side=_pos_side)
                                sl_placed = True
                                break
                            except Exception as _e:
                                print(f"  ⚠️  [{symbol}] SL BE attempt {_attempt+1}/5 gagal: {_e}")
                                if _attempt < 4:
                                    time.sleep(0.5 * (_attempt + 1))  # delay naik: 0.5s, 1s, 1.5s, 2s
                        # FIX KRITIS: jika SL tetap gagal setelah 5 attempt → tandai posisi
                        # "unprotected" agar scan loop berikutnya TERUS retry pasang SL/TP,
                        # bukan diam selamanya (sebelumnya posisi bisa lari tanpa SL/TP
                        # permanen karena tidak masuk cabang manapun di iterasi berikutnya).
                        if not sl_placed:
                            with _positions_lock:
                                if symbol in active_positions:
                                    active_positions[symbol]["unprotected"] = True
                            # FIX KRITIS: save_state() WAJIB dipanggil segera setelah flag
                            # unprotected di-set. Sebelumnya flag ini cuma hidup di memori —
                            # kalau bot direstart (manual/crash) SEBELUM retry berikutnya
                            # berhasil pasang SL, load_state() saat boot membaca file lama
                            # yang unprotected-nya masih False, dan sync_existing_positions()
                            # skip verifikasi SL untuk symbol yang sudah ada di active_positions
                            # → posisi jalan tanpa SL tanpa terdeteksi sampai kena kerugian.
                            save_state()
                            send_telegram_raw(
                                f"🚨 <b>SL BE GAGAL DIPASANG — {symbol}</b>\n"
                                f"{'─'*34}\n"
                                f"TP1 sudah kena tapi SL Break Even gagal setelah 5 attempt.\n"
                                f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry}\n"
                                f"⚠️ Posisi sisa LOT jalan TANPA SL! Bot akan terus retry tiap scan.\n"
                                f"Pasang manual jika alert ini berulang!"
                            )
                        # ── FIX: TP2 TIDAK PERNAH dibatalkan di atas (cancel_sl_orders_only
                        # hanya menyentuh SL), jadi order TP2 asli beserta tp2_order_ref-nya
                        # masih hidup persis seperti sebelum TP1 hit — tidak perlu dipasang
                        # ulang di sini. Kalaupun TP2 order-nya sendiri ternyata ke-cancel
                        # atau hilang karena sebab lain (manual di app Binance, gangguan sisi
                        # exchange, dll — bukan gara-gara proses ini), lapisan self-heal TP2
                        # di bawah (pengecekan status tiap scan) yang akan pasang ulang.
                        if sl_placed:
                            # FIX race condition: guard write dengan lock + cek symbol masih ada
                            with _positions_lock:
                                if symbol in active_positions:
                                    active_positions[symbol]["sl"]              = new_sl
                                    active_positions[symbol]["trailing_active"] = True
                                    active_positions[symbol]["unprotected"]     = False
                            if symbol in active_positions:
                                notify_trailing_activated(symbol, active_positions[symbol], new_sl)
                    except Exception as e:
                        print(f"⚠️ Gagal pindah SL ke BE {symbol}: {e}")
                        with _positions_lock:
                            if symbol in active_positions:
                                active_positions[symbol]["unprotected"] = True
                        save_state()  # FIX KRITIS: persist unprotected segera, tahan restart

            # ── FIX: self-heal ghost-TP2 — sebelumnya TP2 cuma dideteksi lewat
            # jalur close-detection (price >= tp2 saat posisi sudah tertutup),
            # jadi kalau order TP2 asli ke-cancel DIAM-DIAM di Binance sebelum
            # harga sampai level TP2 (race dengan cancel_open_orders(), orphan
            # cleanup, dsb), posisi jalan tanpa TP2 sampai giliran SL — tidak
            # ada yang mengecek order TP2 masih hidup atau tidak selagi posisi
            # masih terbuka. Sekarang: cek status order TP2 tiap scan (mirip
            # TP1) dan pasang ulang otomatis kalau ternyata CANCELED/EXPIRED.
            if pos.get("tp1_hit") and not pos.get("_tp2_healing"):
                _tp2_ref_chk = pos.get("tp2_order_ref")
                _tp2_size_chk = pos.get("tp2_size", 0)
                _min_qty_chk  = pos.get("min_qty", 0.001)
                if _tp2_ref_chk and _tp2_size_chk >= _min_qty_chk:
                    _tp2_status_chk = check_order_status_direct(symbol, _tp2_ref_chk)
                    if _tp2_status_chk in ("CANCELED",):
                        print(f"  ⚠️  [{symbol}] Order TP2 asli berstatus CANCELED/EXPIRED "
                              f"di Binance (bukan FILLED) — posisi kehilangan TP2, pasang ulang...")
                        try:
                            with _positions_lock:
                                if symbol in active_positions:
                                    active_positions[symbol]["_tp2_healing"] = True
                            # FIX RACE: lock sama dengan cleanup_duplicate_position_orders()
                            # dan self-heal TP1 — cegah cleanup mencancel TP2 baru ini di
                            # tengah proses pasang-ulang.
                            with _order_mgmt_lock:
                                _pos_side_heal2 = pos.get("position_side", "BOTH")
                                _fresh_heal2 = get_current_price(symbol) or price
                                _tp2_price_heal = adjust_tp_price(tp2, _fresh_heal2, direction, tick_size, buffer_pct=0.002)
                                _new_tp2_ref2 = algo_post_tp(symbol, sl_side, _tp2_price_heal, _tp2_size_chk, position_side=_pos_side_heal2)
                                with _positions_lock:
                                    if symbol in active_positions:
                                        active_positions[symbol]["tp2_order_ref"] = _new_tp2_ref2
                                        active_positions[symbol]["_tp2_healing"] = False
                                save_state()
                            send_telegram_raw(
                                f"🔧 <b>TP2 DIPASANG ULANG — {symbol}</b>\n"
                                f"Order TP2 asli ke-cancel di Binance tanpa fill. Sudah dipasang ulang otomatis."
                            )
                            print(f"  ✅ [{symbol}] TP2 berhasil dipasang ulang setelah CANCELED/EXPIRED.")
                        except Exception as _heal2_e:
                            print(f"  ⚠️  [{symbol}] Gagal pasang ulang TP2 setelah CANCELED: {_heal2_e}")
                            with _positions_lock:
                                if symbol in active_positions:
                                    active_positions[symbol]["_tp2_healing"] = False
                            send_telegram_raw(
                                f"🚨 <b>TP2 HILANG & GAGAL DIPASANG ULANG — {symbol}</b>\n"
                                f"Order TP2 ke-cancel di Binance dan bot gagal pasang ulang.\n"
                                f"Error: <code>{_heal2_e}</code>\nCek dan pasang manual jika perlu!"
                            )
                    # status "FILLED"/"NEW"/None → ditangani jalur lain (close-detection / retry nanti)

            # ── FIX B: TP2_WAIT_MINUTES timeout — close market jika TP2 tidak kena ──
            # Setelah TP1 hit, kalau TP2 tidak kena dalam TP2_WAIT_MINUTES menit → close paksa.
            # Mencegah posisi floating terlalu lama di BE tanpa progress menuju TP2.
            if TP2_WAIT_MINUTES > 0 and pos.get("tp1_hit") and not pos.get("tp2_timeout_notified"):
                _tp1_hit_at_str = pos.get("tp1_hit_at")
                if _tp1_hit_at_str:
                    try:
                        _tp1_hit_at = datetime.fromisoformat(_tp1_hit_at_str)
                        if _tp1_hit_at.tzinfo is None:
                            _tp1_hit_at = _tp1_hit_at.replace(tzinfo=timezone.utc)
                        _elapsed_min = (datetime.now(timezone.utc) - _tp1_hit_at).total_seconds() / 60
                        if _elapsed_min >= TP2_WAIT_MINUTES:
                            print(f"  ⏰ [{symbol}] TP2 timeout ({TP2_WAIT_MINUTES} menit) — close market paksa")
                            _tp2_remaining = pos.get("tp2_size") or pos.get("lot", 0)
                            _pos_side_t    = pos.get("position_side", "BOTH")
                            try:
                                cancel_open_orders(symbol)
                                time.sleep(0.2)
                                # FIX 1: format quantity pakai format_qty (bukan raw float → -1111)
                                _t_filters    = get_lot_filters(symbol)
                                _t_mkt_step   = _t_filters.get("mkt_stepSize", _t_filters.get("stepSize", 0.001))
                                _t_qty_prec   = _t_filters.get("quantityPrecision", None)
                                _t_mkt_max    = _t_filters.get("mkt_maxQty", _t_filters.get("maxQty", 9999999.0))
                                _tp2_qty      = min(_tp2_remaining, _t_mkt_max)
                                _tp2_qty_str  = format_qty(_tp2_qty, _t_mkt_step, _t_qty_prec)
                                # FIX 2: hedge mode → positionSide=LONG/SHORT tidak boleh pakai reduceOnly (-4046)
                                _close_params = {
                                    "symbol":       symbol,
                                    "side":         sl_side,
                                    "type":         "MARKET",
                                    "quantity":     _tp2_qty_str,
                                    "positionSide": _pos_side_t,
                                }
                                if _pos_side_t not in ("LONG", "SHORT"):
                                    _close_params["reduceOnly"] = "true"
                                api_post("/fapi/v1/order", _close_params)
                                # FIX 3: hapus dari active_positions setelah close berhasil
                                # agar slot trade segera bebas & tidak ada double-notif
                                _real_pnl_timeout = (price - pos["entry"]) * _tp2_qty if pos["direction"] == "LONG" \
                                                    else (pos["entry"] - price) * _tp2_qty
                                if _real_pnl_timeout <= 0:
                                    _real_pnl_timeout = 0.001  # dihitung WIN karena TP1 sudah hit
                                update_performance(_real_pnl_timeout)
                                set_cooldown_post_close(symbol, reason="TP2 timeout close", is_stoploss=False)
                                with _positions_lock:
                                    active_positions.pop(symbol, None)
                                save_state()   # FIX: sinkron ke disk begitu posisi close (TP2 timeout)
                                send_telegram_raw(
                                    f"⏰ <b>TP2 Timeout — {symbol}</b>\n"
                                    f"{'─'*34}\n"
                                    f"TP2 tidak tercapai dalam {TP2_WAIT_MINUTES} menit setelah TP1.\n"
                                    f"Sisa posisi ({_tp2_qty_str} lot) di-close market di harga {price}.\n"
                                    f"✅ Trade dihitung WIN (TP1 sudah ambil profit)."
                                )
                            except Exception as _te:
                                print(f"  ⚠️  [{symbol}] TP2 timeout close gagal: {_te}")
                                # FIX: cancel_open_orders sudah jalan sebelum close attempt ini —
                                # kalau close-nya gagal, SL/TP lama sudah hilang tapi posisi masih
                                # terbuka tanpa proteksi. Tandai unprotected supaya cabang retry
                                # di bawah pasang ulang SL, bukan cuma diam sampai scan berikutnya
                                # masuk blok timeout ini lagi (yang cuma coba MARKET close lagi,
                                # bukan pasang SL pengaman sementara).
                                with _positions_lock:
                                    if symbol in active_positions:
                                        active_positions[symbol]["unprotected"] = True
                                save_state()  # FIX KRITIS: persist unprotected segera, tahan restart
                                send_telegram_raw(
                                    f"⚠️ <b>TP2 Timeout Close GAGAL — {symbol}</b>\n"
                                    f"Error: <code>{_te}</code>\n"
                                    f"Cek & close manual jika posisi masih terbuka!"
                                )
                    except Exception:
                        pass

            elif pos.get("unprotected"):
                # FIX: sebelumnya kondisi ini `tp1_hit and unprotected and not
                # trailing_active` — jadi kalau posisi SUDAH trailing_active lalu
                # gagal re-place SL saat trailing geser (lihat cabang trailing di
                # bawah), flag unprotected TIDAK PERNAH masuk cabang retry ini
                # (karena trailing_active sudah True), malah selalu balik ke
                # cabang trailing biasa yang cuma retry KALAU should_update True
                # lagi. Sekarang: unprotected saja cukup jadi trigger retry,
                # apapun status trailing_active-nya.

                # ── FIX KRITIS: retry pasang SL BE + TP2 untuk posisi yang sempat
                # gagal terlindungi (lihat blok TP1 di atas). Tanpa cabang ini,
                # posisi yang gagal placement SL-nya akan berjalan TANPA SL/TP
                # selamanya karena tidak masuk kondisi manapun di scan berikutnya.
                print(f"  🔁 [{symbol}] Posisi unprotected — retry pasang SL BE + TP2")
                try:
                    _remaining_lot = pos.get("tp2_size") or pos.get("lot", 0)
                    _pos_side = pos.get("position_side", "BOTH")
                    sl_placed = False
                    new_sl = adjust_sl_price(be_target_price(entry, direction), price, direction, tick_size, buffer_pct=0.005)
                    for _attempt, _buf in enumerate((0.000, 0.005, 0.010, 0.015, 0.020)):
                        try:
                            fresh = get_current_price(symbol) or price
                            new_sl = adjust_sl_price(be_target_price(entry, direction), fresh, direction, tick_size, buffer_pct=max(_buf, 0.005))
                            algo_post_sl(symbol, sl_side, new_sl, quantity=_remaining_lot, position_side=_pos_side)
                            sl_placed = True
                            break
                        except Exception as _e:
                            print(f"  ⚠️  [{symbol}] retry SL BE attempt {_attempt+1}/5 gagal: {_e}")
                            if _attempt < 4:
                                time.sleep(0.5 * (_attempt + 1))
                    if sl_placed:
                        _tp2_size = pos.get("tp2_size", 0)
                        _min_qty  = pos.get("min_qty", 0.001)
                        # FIX: jangan paksa ke min_qty kalau sisa (lot - tp1_size)
                        # genuinely ~0 (lot sudah digabung semua ke TP1 saat entry).
                        if _tp2_size < _min_qty:
                            _tp1_size = pos.get("tp1_size", 0)
                            _remaining = pos.get("lot", 0) - _tp1_size
                            _tp2_size = round_lot_to_step(_remaining, pos.get("mkt_step_size", step_size)) \
                                        if _remaining >= _min_qty else 0.0
                        if _tp2_size >= _min_qty:
                            try:
                                _tp2_price = adjust_tp_price(tp2, price, direction, tick_size, buffer_pct=0.002)
                                algo_post_tp(symbol, sl_side, _tp2_price, _tp2_size, position_side=_pos_side)
                            except Exception as _tp2e:
                                print(f"  ⚠️  [{symbol}] TP2 retry gagal dipasang: {_tp2e}")
                                send_telegram_raw(f"⚠️ <b>TP2 gagal dipasang — {symbol}</b>\nCek manual di Binance!\nError: <code>{_tp2e}</code>")
                        with _positions_lock:
                            if symbol in active_positions:
                                active_positions[symbol]["sl"]              = new_sl
                                active_positions[symbol]["trailing_active"] = True
                                active_positions[symbol]["unprotected"]     = False
                        if symbol in active_positions:
                            notify_trailing_activated(symbol, active_positions[symbol], new_sl)
                        send_telegram_raw(f"✅ <b>SL BE berhasil dipasang ulang — {symbol}</b>\nPosisi sudah terlindungi kembali.")
                    else:
                        print(f"  🚨 [{symbol}] Posisi MASIH unprotected — akan retry lagi scan berikutnya")
                except Exception as e:
                    print(f"⚠️ Retry unprotected gagal {symbol}: {e}")

            elif pos.get("trailing_active"):
                # ── FIX BUG 1: gunakan original_sl untuk hitung RR, bukan sl terkini ──
                # Setelah BE, sl = entry → denominator hampir 0 → rr = INF → trailing geser tiap scan
                # Fix: simpan original_sl saat entry dan pakai itu sebagai basis RR
                original_sl = pos.get("original_sl", sl)
                original_sl_dist = abs(entry - original_sl)
                if original_sl_dist < 1e-9:
                    original_sl_dist = abs(entry - sl) + 1e-9  # fallback ke sl terkini jika original tidak ada
                rr = abs(price - entry) / original_sl_dist
                if rr >= TRAILING_TRIGGER:
                    if direction == "LONG":
                        candidate_raw = price * (1 - 0.005)
                    else:
                        candidate_raw = price * (1 + 0.005)
                    candidate_sl = adjust_sl_price(candidate_raw, price, direction, tick_size, buffer_pct=0.005)
                    should_update = (direction == "LONG"  and candidate_sl > sl) or \
                                   (direction == "SHORT" and candidate_sl < sl)
                    if should_update:
                        try:
                            # ── FIX: sama seperti fix di titik TP1-hit — sebelumnya
                            # cancel_open_orders(symbol) di sini membatalkan SL DAN TP2
                            # sekaligus tiap kali trailing geser, padahal TP2 levelnya
                            # tidak berubah. Sekarang cuma cancel SL; TP2 tetap hidup
                            # tanpa jeda.
                            cancel_sl_orders_only(symbol)
                            time.sleep(0.3)
                            _remaining_lot = pos.get("tp2_size") or pos.get("lot", 0)
                            _pos_side2 = pos.get("position_side", "BOTH")
                            sl_ok = False
                            for _tbuf in (0.000, 0.005, 0.010):
                                try:
                                    _fresh2 = get_current_price(symbol) or price
                                    _csl    = adjust_sl_price(candidate_raw, _fresh2, direction, tick_size, buffer_pct=max(_tbuf, 0.005))
                                    algo_post_sl(symbol, sl_side, _csl, quantity=_remaining_lot, position_side=_pos_side2)
                                    candidate_sl = _csl
                                    sl_ok = True
                                    break
                                except Exception as _sle:
                                    print(f"  ⚠️  [{symbol}] trailing SL re-place attempt gagal ({_tbuf}): {_sle}")
                                    time.sleep(0.3)
                            # FIX KRITIS: SEBELUMNYA kalau ke-3 attempt di atas gagal semua,
                            # kode ini `raise` — lompat ke except di paling bawah yang cuma
                            # `print(...)` dan TIDAK menandai posisi unprotected. Karena
                            # cancel_sl_orders_only(symbol) SUDAH terlanjur jalan di atas, hasilnya
                            # posisi kehilangan SL LAMA tanpa SL BARU terpasang, TIDAK ada alert,
                            # TIDAK ada retry otomatis di scan berikutnya (trailing_active tetap
                            # True sehingga selalu masuk cabang ini lagi, dan hanya retry kalau
                            # `should_update` True lagi — kalau harga diam/mundur dikit, posisi
                            # bisa jalan TANPA SL tanpa batas waktu tanpa siapapun tahu).
                            # FIX: jangan raise — tandai unprotected + alert + biarkan cabang
                            # "unprotected" di scan berikutnya yang retry terus sampai berhasil.
                            if not sl_ok:
                                with _positions_lock:
                                    if symbol in active_positions:
                                        active_positions[symbol]["unprotected"] = True
                                save_state()  # FIX KRITIS: persist unprotected segera, tahan restart
                                send_telegram_raw(
                                    f"🚨 <b>SL TRAILING GAGAL DIPASANG ULANG — {symbol}</b>\n"
                                    f"{'─'*34}\n"
                                    f"SL lama sudah ke-cancel, SL baru gagal terpasang setelah 3 attempt.\n"
                                    f"{'🟢' if direction == 'LONG' else '🔴'} {direction} | Entry: {entry} | Mark: {price}\n"
                                    f"⚠️ Posisi jalan TANPA SL! Bot akan terus retry tiap scan.\n"
                                    f"Pasang manual jika alert ini berulang!"
                                )
                            # ── FIX: TP2 TIDAK PERNAH dibatalkan di atas (cancel_sl_orders_only
                            # hanya menyentuh SL) — tidak perlu dipasang ulang di sini lagi.
                            # Order TP2 asli tetap hidup persis seperti sebelumnya; kalau ternyata
                            # hilang karena sebab lain, self-heal TP2 (cek status tiap scan) yang
                            # akan menanganinya.
                            if sl_ok:
                                # FIX race condition: guard write dengan lock + cek symbol masih ada
                                with _positions_lock:
                                    if symbol in active_positions:
                                        active_positions[symbol]["sl"] = candidate_sl
                                send_telegram_raw(
                                    f"🔄 <b>Trailing geser — {symbol}</b>\n"
                                    f"SL: {sl} → <b>{candidate_sl}</b> | Mark: {price}\n"
                                    f"RR: {rr:.2f}x dari SL awal"
                                )
                        except Exception as e:
                            print(f"⚠️ Gagal geser trailing {symbol}: {e}")

        except Exception as e:
            print(f"⚠️ manage_trailing error {symbol}: {e}")


# ── FIX KRITIS — Background thread khusus TP1/TP2/Trailing BE ───────────────
# ROOT CAUSE bug "TP1/TP2 tidak ngejual sama sekali" & "trailing SL breakeven
# tidak ketrigger": sebelumnya manage_trailing() hanya dipanggil SEKALI per
# iterasi main loop, SETELAH proses scan sinyal (puluhan pair, ThreadPoolExecutor
# fetch OHLCV) selesai. Kalau scan lambat, jeda nyata antar-pengecekan trailing
# bisa jauh melebihi SCAN_INTERVAL (60 detik) — harga sempat menembus TP1/TP2
# lalu berbalik ke SL sebelum bot sempat cek & pasang SL BE / TP2.
#
# FIX: manage_trailing() sekarang jalan di thread sendiri, polling tiap
# TRAILING_POLL_INTERVAL detik (default 8s), TIDAK menunggu scan sinyal.
# Aman dijalankan concurrent dengan main loop karena:
#   - _positions_lock melindungi semua read/write active_positions
#   - _BINANCE_SEMAPHORE membatasi 1 request paralel ke Binance (tidak akan
#     bentrok/race dengan API call dari thread scan sinyal)
_trailing_thread_started = False

def _trailing_monitor_loop():
    """Loop background: cek TP1/TP2/trailing BE tiap beberapa detik, independen
    dari durasi scan sinyal di main loop."""
    while True:
        try:
            if not bot_paused:
                manage_trailing()
        except Exception as e:
            print(f"⚠️ _trailing_monitor_loop error: {e}")
        time.sleep(TRAILING_POLL_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 19b — HOURLY POSITION REPORT (IMPROVED)
# ═══════════════════════════════════════════════════════════════════════════

_last_hourly_report: datetime | None = None
_last_orphan_cleanup: datetime | None = None
# Interval auto-cleanup orphan/duplicate orders.
# FIX: sebelumnya 5 menit (300s) — di testnet, /fapi/v2/positionRisk sering 408
# timeout (server-side, testnet memang flaky), dan tiap gagal kena retry loop
# 4x @ up to 10s = sampai ~40 detik per attempt, lalu (dulu) selalu lapor gagal
# ke Telegram walau ini auto-cleanup, bukan command manual → jadi spam.
# Sekarang: interval diperlonggar jadi 30 menit (cukup untuk cegah duplicate SL/TP
# numpuk parah) supaya jauh lebih jarang nabrak jendela flaky testnet, DAN
# notifikasi gagal auto-cleanup sudah di-silent-kan (lihat silent_if_clean check
# di cleanup_stale_orders / cleanup_duplicate_position_orders) — kalau tetap gagal,
# cuma log ke console, tidak lagi spam Telegram.
AUTO_CLEANUP_INTERVAL_SECONDS = 1800  # 30 menit


def auto_cleanup_orphan_orders():
    """
    Cek & bersihkan orphan orders (SL/TP nyangkut tanpa posisi aktif) DAN
    duplicate SL/TP orders (menumpuk di posisi yang masih aktif) otomatis
    setiap 5 menit, tanpa perlu command manual. Silent kalau memang bersih.
    """
    global _last_orphan_cleanup
    now = datetime.now(timezone.utc)
    if _last_orphan_cleanup is not None:
        elapsed = (now - _last_orphan_cleanup).total_seconds()
        if elapsed < AUTO_CLEANUP_INTERVAL_SECONDS:
            return
    _last_orphan_cleanup = now
    try:
        cleanup_stale_orders(dry_run=False, silent_if_clean=True)
    except Exception as e:
        print(f"⚠️ Auto cleanup orphan orders error: {e}")
    try:
        cleanup_duplicate_position_orders(dry_run=False, silent_if_clean=True)
    except Exception as e:
        print(f"⚠️ Auto cleanup duplicate orders error: {e}")


def refresh_pnl_snapshots(current_balance: float):
    """
    Cek dan reset snapshot balance harian & bulanan jika perlu.
    Dipanggil setiap kali kita punya balance terbaru.
    """
    now        = datetime.now(timezone.utc)
    today_str  = now.strftime("%Y-%m-%d")
    month_str  = now.strftime("%Y-%m")

    if bot_state["balance_day_date"] != today_str:
        bot_state["balance_day_start"] = current_balance
        bot_state["balance_day_date"]  = today_str
        bot_state["day_wins"]          = 0
        bot_state["day_losses"]        = 0

    if bot_state["balance_month_key"] != month_str:
        bot_state["balance_month_start"] = current_balance
        bot_state["balance_month_key"]   = month_str
        bot_state["month_wins"]          = 0
        bot_state["month_losses"]        = 0


def get_pnl_summary_lines(current_balance: float) -> str:
    """
    Buat baris ringkasan PNL harian & bulanan.
    Format singkat untuk /pnl dan hourly report.
    """
    lines = []

    # ── Harian ──────────────────────────────────────────────────────────────
    day_start = bot_state.get("balance_day_start", 0)
    if day_start > 0:
        day_pnl     = current_balance - day_start
        day_pnl_pct = (day_pnl / day_start) * 100
        day_em      = "🟢" if day_pnl >= 0 else "🔴"
        lines.append(
            f"{day_em} Hari ini  : <b>{'+'if day_pnl>=0 else ''}{day_pnl:.2f} USDT "
            f"({'+'if day_pnl_pct>=0 else ''}{day_pnl_pct:.2f}%)</b>"
        )

    # ── Bulanan ──────────────────────────────────────────────────────────────
    month_start = bot_state.get("balance_month_start", 0)
    if month_start > 0:
        month_pnl     = current_balance - month_start
        month_pnl_pct = (month_pnl / month_start) * 100
        month_em      = "🟢" if month_pnl >= 0 else "🔴"
        month_key     = bot_state.get("balance_month_key", "")
        lines.append(
            f"{month_em} Bulan ini : <b>{'+'if month_pnl>=0 else ''}{month_pnl:.2f} USDT "
            f"({'+'if month_pnl_pct>=0 else ''}{month_pnl_pct:.2f}%)</b>"
        )

    return "\n".join(lines) if lines else ""



def get_mode_pnl_block() -> str:
    """
    Buat blok ringkasan PnL kumulatif terpisah per mode (LIVE / DEMO).
    Dipanggil di /pnl dan /status.
    """
    lines = []
    for mode, em in [("LIVE", "🔴"), ("DEMO", "🟢")]:
        data      = bot_state["mode_pnl"].get(mode, {})
        realized  = data.get("realized", 0.0)
        start_bal = data.get("start_bal", 0.0)
        wins      = data.get("wins", 0)
        losses    = data.get("losses", 0)
        total     = wins + losses
        wr        = f"{wins/total*100:.0f}%" if total > 0 else "–"
        pct       = f"({'+'if realized>=0 else ''}{realized/start_bal*100:.2f}%)" if start_bal > 0 else ""
        sign      = "+" if realized >= 0 else ""
        active    = " ◀ aktif" if mode == BOT_MODE else ""
        lines.append(
            f"{em} <b>{mode}</b>{active}\n"
            f"   PnL Kumulatif : <b>{sign}{realized:.2f} USDT {pct}</b>\n"
            f"   W/L           : {wins}W / {losses}L (WR {wr})"
        )
    return "\n".join(lines)


def cmd_resetpnl():
    """
    /resetpnl
    Reset PnL kumulatif mode yang sedang aktif (LIVE atau DEMO).
    Balance snapshot juga di-reset ke saldo saat ini.
    """
    try:
        bal = get_total_balance()
    except Exception:
        bal = 0.0
    bot_state["mode_pnl"][BOT_MODE] = {
        "realized":  0.0,
        "start_bal": bal,
        "wins":      0,
        "losses":    0,
    }
    em = "🔴" if BOT_MODE == "LIVE" else "🟢"
    send_telegram_raw(
        f"{em} <b>PnL {BOT_MODE} Di-reset</b>\n"
        f"{'─'*34}\n"
        f"💰 Balance saat ini : <b>{bal:.2f} USDT</b>\n"
        f"PnL kumulatif {BOT_MODE} mulai dihitung dari nol."
    )
    print(f"⚙️ PnL {BOT_MODE} di-reset. Balance snapshot: {bal:.2f} USDT")


def cmd_resetstate(parts: list):
    """
    /resetstate confirm
    Hapus bot_state.json dari disk supaya bot_paused, TP1_PARTIAL, custom score
    threshold, cooldown map, signal hash, dsb kembali ke default kode — TANPA
    perlu hapus-buat-ulang project Railway (env var API/Telegram tetap utuh,
    karena itu env var bukan bot_state.json).

    Ditambahkan 2026-09-15 sebagai jalan keluar untuk kasus TP1_PARTIAL=1.0
    (atau setting lain) yang kepersist dari sesi lama dan bikin bot jalan
    beda dari default kode setelah restart — daripada hapus project, cukup
    /resetstate confirm lalu redeploy/restart proses.

    Wajib ketik "confirm" dan TIDAK BOLEH ada posisi aktif — reset state saat
    ada posisi live bisa bikin bot lupa SL/TP/trailing yang sedang berjalan
    di posisi itu (state di memory langsung dipakai lagi tanpa restart, jadi
    guard ini benar-benar mencegah, bukan cuma peringatan).
    """
    if active_positions:
        send_telegram_raw(
            f"🚫 <b>/resetstate dibatalkan</b>\n"
            f"{'─'*34}\n"
            f"Masih ada {len(active_positions)} posisi aktif: "
            f"{', '.join(sorted(active_positions.keys()))}\n"
            f"Tutup/tunggu semua posisi selesai dulu sebelum reset state — "
            f"kalau tidak, bot bisa kehilangan jejak SL/TP/trailing posisi ini."
        )
        return

    confirm = len(parts) > 1 and parts[1].lower() == "confirm"
    if not confirm:
        send_telegram_raw(
            f"⚠️ <b>/resetstate — konfirmasi diperlukan</b>\n"
            f"{'─'*34}\n"
            f"Ini akan MENGHAPUS bot_state.json (bot_paused, TP1_PARTIAL, "
            f"custom threshold score, cooldown, mode filter, dll — SEMUA "
            f"kembali ke default kode). Env var API key/Telegram di Railway "
            f"TIDAK terpengaruh.\n\n"
            f"Ketik <code>/resetstate confirm</code> untuk lanjut, lalu "
            f"restart bot secara manual dari Railway (Redeploy/Restart) "
            f"supaya default kode benar-benar dipakai ulang."
        )
        return

    try:
        if os.path.exists(_STATE_FILE):
            os.remove(_STATE_FILE)
            send_telegram_raw(
                f"✅ <b>bot_state.json dihapus</b>\n"
                f"{'─'*34}\n"
                f"Sekarang <b>restart bot manual</b> dari Railway "
                f"(Redeploy/Restart) — setelah restart, semua setting "
                f"kembali ke default kode (mis. TP1_PARTIAL=0.5, mode 2-TP)."
            )
            print(f"🗑️  bot_state.json dihapus via /resetstate — menunggu restart manual.")
        else:
            send_telegram_raw("ℹ️ bot_state.json memang belum ada — tidak ada yang dihapus.")
    except Exception as e:
        send_telegram_raw(f"❌ Gagal hapus bot_state.json: <code>{e}</code>")


def get_unrealized_pnl(symbol: str, pos: dict) -> float | None:
    try:
        price = get_current_price(symbol)
        if price is None:
            return None
        entry = pos["entry"]
        lot   = pos["lot"]
        if pos["direction"] == "LONG":
            return (price - entry) * lot
        else:
            return (entry - price) * lot
    except Exception:
        return None


def _build_position_line(symbol: str, pos: dict, now: datetime,
                         binance_data: dict | None = None) -> tuple[str, float]:
    """
    Buat satu baris laporan posisi. Return (teks, pnl_float).

    binance_data: dict dari /fapi/v2/positionRisk untuk symbol ini (opsional).
    Jika diberikan, entry price dan PnL diambil langsung dari Binance
    (sama persis dengan /pnl command) agar kedua laporan selalu konsisten.
    Jika None, fallback ke kalkulasi manual via get_current_price().
    """
    direction = pos["direction"]
    sl        = pos["sl"]
    tp1       = pos["tp1"]
    tp2       = pos["tp2"]
    lot       = pos["lot"]
    dir_em    = "🟢" if direction == "LONG" else "🔴"
    tp1_em    = "✅" if pos.get("tp1_hit") else "⏳"
    trail_em  = "🔄" if pos.get("trailing_active") else "  "
    pre_em    = "📥" if pos.get("pre_existing") else "🤖"

    # Durasi
    open_time = pos.get("open_time")
    if open_time:
        dur_secs = int((now - open_time).total_seconds())
        dur_h    = dur_secs // 3600
        dur_m    = (dur_secs % 3600) // 60
        dur_str  = f"{dur_h}j {dur_m}m"
    else:
        dur_str = "pre-existing"

    # ── Harga & PnL: prioritaskan data Binance jika tersedia ─────────────────
    pnl_val   = 0.0
    pnl_str   = "❓ N/A"
    pnl       = None

    if binance_data is not None:
        # Sumber terpercaya: langsung dari Binance positionRisk
        entry     = float(binance_data["entryPrice"])
        price     = float(binance_data["markPrice"])
        pnl       = float(binance_data["unRealizedProfit"])
        pnl_val   = pnl
        price_str = f"{price:,.4f}"
        pnl_pct   = (pnl / (entry * lot + 1e-9)) * 100
        pnl_em    = "🟢" if pnl >= 0 else "🔴"
        pnl_str   = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT ({'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%)"
    else:
        # Fallback: kalkulasi manual (posisi manual / tidak ada di positionRisk)
        entry     = pos["entry"]
        price     = get_current_price(symbol)
        price_str = "N/A"
        if price is not None:
            price_str = f"{price:,.4f}"
            if direction == "LONG":
                pnl = (price - entry) * lot
            else:
                pnl = (entry - price) * lot
            pnl_val = pnl
            pnl_pct = (pnl / (entry * lot + 1e-9)) * 100
            pnl_em  = "🟢" if pnl >= 0 else "🔴"
            pnl_str = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT ({'+' if pnl_pct>=0 else ''}{pnl_pct:.1f}%)"

    # Jarak ke SL dan TP2
    dist_sl  = abs(price - sl)  / (price + 1e-9) * 100 if price else 0
    dist_tp2 = abs(tp2 - price) / (price + 1e-9) * 100 if price else 0

    line = (
        f"{dir_em}{pre_em} <b>{symbol}</b> {direction} | Lot: {lot}\n"
        f"  💰 Entry   : {entry}  →  🔖 Harga: {price_str}\n"
        f"  🛑 SL      : {sl}  ({dist_sl:.1f}% away)  {trail_em}\n"
    )
    # FIX: TP1_PARTIAL=1.0 berarti mode single-TP (full close di satu target),
    # jadi tp1 == tp2 secara nilai. Sebelumnya kode ini SELALU cetak baris
    # TP1 dan TP2 terpisah walau nilainya identik — bikin notif kelihatan
    # ada 2 target padahal cuma 1 order TP yang benar-benar terpasang di
    # Binance (lihat "SL + TP tunggal sudah terpasang" di notif limit fill).
    if abs(tp1 - tp2) < 1e-12:
        line += f"  🎯 TP      : {tp2}  ({dist_tp2:.1f}% away) {tp1_em}\n"
    else:
        line += (
            f"  🎯 TP1     : {tp1} {tp1_em}\n"
            f"  🎯 TP2     : {tp2}  ({dist_tp2:.1f}% away)\n"
        )
    line += (
        f"  📊 PnL     : {pnl_str}\n"
        f"  ⏱ Durasi  : {dur_str}"
    )
    return line, pnl_val if pnl is not None else None


def send_hourly_position_report():
    global _last_hourly_report

    now = datetime.now(timezone.utc)

    if _last_hourly_report is not None:
        elapsed = (now - _last_hourly_report).total_seconds()
        if elapsed < 3600:
            return

    _last_hourly_report = now
    now_str = now.strftime("%d %b %Y %H:%M UTC")

    if not active_positions:
        try:
            current_bal = get_total_balance()
            # FIX: skip refresh jika None (API error)
            if current_bal is not None:
                refresh_pnl_snapshots(current_bal)
                pnl_summary = get_pnl_summary_lines(current_bal)
                bal_line    = f"💼 Balance : <b>{current_bal:.2f} USDT</b>\n"
            else:
                pnl_summary = ""
                bal_line    = "💼 Balance : <b>N/A (API error)</b>\n"
        except Exception:
            pnl_summary = ""
            bal_line    = ""

        mode_pnl_block = get_mode_pnl_block()
        cumulative_block = (
            f"{'─'*38}\n"
            f"📈 <b>Cumulative PNL</b>\n"
            f"{pnl_summary}\n"
            f"{'─'*38}\n"
            f"📊 <b>PnL per Mode</b>\n"
            f"{mode_pnl_block}\n"
            f"💡 /resetpnl — reset PnL mode aktif"
        ) if (pnl_summary or True) else ""

        msg = (
            f"📋 <b>HOURLY REPORT — {now_str}</b>\n"
            f"{'─'*38}\n"
            f"📭 Tidak ada posisi aktif.\n"
            f"{bal_line}"
            f"{'─'*38}\n"
            f"{cumulative_block}"
            f"⏰ Next report ~1 jam"
        )
        send_telegram_raw(msg)
        return

    total_pnl  = 0.0
    pnl_failed = []
    lines      = []

    # Fetch semua posisi dari Binance sekaligus — sumber yang sama dengan /pnl
    # agar entry price dan PnL selalu konsisten antara hourly report dan /pnl.
    _binance_pos_map: dict = {}
    try:
        _all_risk = api_get("/fapi/v2/positionRisk", signed=True)
        for _p in _all_risk:
            if float(_p.get("positionAmt", 0)) != 0:
                _binance_pos_map[_p["symbol"]] = _p
    except Exception as _e:
        print(f"⚠️ Hourly report: gagal fetch positionRisk dari Binance ({_e}), fallback ke kalkulasi manual")

    for symbol, pos in active_positions.items():
        _bdata = _binance_pos_map.get(symbol)  # None = fallback ke kalkulasi manual
        line, pnl_val = _build_position_line(symbol, pos, now, binance_data=_bdata)
        lines.append(line)
        if pnl_val is not None:
            total_pnl += pnl_val
        else:
            pnl_failed.append(symbol)

    total_em  = "🟢" if total_pnl >= 0 else "🔴"
    total_str = f"{total_em} {'+'if total_pnl>=0 else ''}{total_pnl:.2f} USDT"

    sep = "\n" + ("─"*34) + "\n"
    positions_block = sep.join(lines)

    try:
        current_bal = get_total_balance()
        # FIX: handle None (API error)
        if current_bal is not None:
            bal_str     = f"{current_bal:.2f} USDT"
            refresh_pnl_snapshots(current_bal)
            pnl_summary = get_pnl_summary_lines(current_bal)
        else:
            bal_str     = "N/A (API error)"
            pnl_summary = ""
    except Exception:
        bal_str     = "N/A"
        pnl_summary = ""

    pre_count  = sum(1 for p in active_positions.values() if p.get("pre_existing"))
    bot_count  = len(active_positions) - pre_count
    pre_note   = f"\n  (📥 {pre_count} pre-existing | 🤖 {bot_count} dari bot)" if pre_count > 0 else ""

    paused_note = "\n⏸ <b>Bot dalam kondisi PAUSED</b>" if bot_paused else ""

    mode_pnl_block = get_mode_pnl_block()
    cumulative_block = (
        f"{'─'*38}\n"
        f"📈 <b>Cumulative PNL</b>\n"
        f"{pnl_summary}\n"
        f"{'─'*38}\n"
        f"📊 <b>PnL per Mode</b>\n"
        f"{mode_pnl_block}\n"
        f"💡 /resetpnl — reset PnL mode aktif"
    ) if (pnl_summary or True) else ""

    msg = (
        f"📋 <b>HOURLY REPORT — {now_str}</b>\n"
        f"{'─'*38}\n"
        f"📌 Posisi Aktif : <b>{len(active_positions)}</b> trade{pre_note}\n"
        f"💼 Balance      : <b>{bal_str}</b>\n"
        f"{'─'*38}\n"
        f"{positions_block}\n"
        f"{'─'*38}\n"
        f"💹 Total Unrealized PnL: <b>{total_str}</b>\n"
        f"{'─'*38}\n"
        f"{cumulative_block}"
        f"⏰ Next report ~1 jam"
        f"{paused_note}"
    )
    send_telegram_raw(msg)
    print(f"📋 Hourly report sent — {len(active_positions)} posisi | PnL: {total_str}")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20 — DRAWDOWN PROTECTION & STATS
# ═══════════════════════════════════════════════════════════════════════════

def check_drawdown():
    """
    Drawdown HARIAN: dihitung dari balance_open hari ini (UTC 00:00), bukan dari
    saldo awal sepanjang masa. Otomatis reset tiap ganti hari lewat
    _reset_daily_limit_for_new_day() (balance_open di-snapshot ulang).

    FIX (2026-10-14): sebelumnya baseline = mode_pnl[mode]["start_bal"] (all-time),
    jadi begitu equity turun >=20% dari saldo awal, bot di-pause lagi SETIAP
    iterasi setelah /start (paused_by di-reset -> check_drawdown trigger ulang)
    dan tidak pernah bisa jalan berhari-hari.
    """
    global bot_paused, _daily_limit_state
    current = get_total_balance()
    # FIX: skip drawdown check jika balance None (API error / IP ban)
    if current is None:
        print("  ⚠️  check_drawdown: balance None (API error) — skip drawdown check")
        return

    # Pastikan snapshot harian sudah untuk hari ini (kalau loop belum sempat reset)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _daily_limit_state.get("date") != today:
        _reset_daily_limit_for_new_day()

    start = _daily_limit_state.get("balance_open", 0.0) or 0.0
    if start <= 0:
        # Belum ada snapshot harian valid -> jangan pause (hindari false trigger)
        return

    dd = (start - current) / start
    if dd >= MAX_DRAWDOWN:
        # Jangan exit() — cukup pause bot agar bisa di-resume via /start atau /resumeorpause
        if _daily_limit_state.get("paused_by") == "DRAWDOWN":
            return  # sudah di-pause sebelumnya, skip notif ulang
        bot_paused = True
        _daily_limit_state["paused_by"] = "DRAWDOWN"
        msg = (
            f"🛑 <b>DRAWDOWN HARIAN TERCAPAI — Bot Di-PAUSE</b>\n"
            f"Drawdown hari ini: <b>{dd*100:.1f}%</b> (limit {MAX_DRAWDOWN*100:.0f}%)\n"
            f"💰 Balance open: {start:.2f} → sekarang: {current:.2f} USDT\n\n"
            f"⏸ Bot otomatis PAUSE — tidak ada trade baru.\n"
            f"🔄 Otomatis aktif lagi di hari baru (00:00 UTC).\n"
            f"Atau ketik /start / /resumeorpause untuk melanjutkan manual."
        )
        print(f"⏸ BOT PAUSED (DAILY DRAWDOWN LIMIT REACHED: {dd*100:.1f}%)")
        send_telegram_raw(msg)
        save_state()


def update_performance(pnl):
    with _perf_lock:   # FIX D: thread-safe — sync_closed_positions & manage_trailing bisa concurrent
        if pnl > 0:
            bot_state["wins"]        += 1
            bot_state["win_streak"]  += 1
            bot_state["lose_streak"]  = 0
            bot_state["day_wins"]    += 1
            bot_state["month_wins"]  += 1
        else:
            bot_state["losses"]      += 1
            bot_state["lose_streak"] += 1
            bot_state["win_streak"]   = 0
            bot_state["day_losses"]  += 1
            bot_state["month_losses"] += 1
        # ── Track PnL kumulatif per mode (LIVE / DEMO) ────────────────────────
        mode_data = bot_state["mode_pnl"].get(BOT_MODE)
        if mode_data is not None:
            mode_data["realized"] += pnl
            if pnl > 0:
                mode_data["wins"] += 1
            else:
                mode_data["losses"] += 1
    # ── Persist state setelah setiap trade close ──────────────────────────
    save_state()


def print_stats():
    total   = bot_state["wins"] + bot_state["losses"]
    winrate = (bot_state["wins"] / total * 100) if total > 0 else 0
    print(f"""
╔══════════════════════════════╗
║        📊 PERFORMANCE BOT AUTO TRADE    ║
╠══════════════════════════════╣
║ Winrate    : {winrate:.2f}%
║ Wins       : {bot_state['wins']}
║ Losses     : {bot_state['losses']}
║ Win Streak : {bot_state['win_streak']}
║ Lose Streak: {bot_state['lose_streak']}
╚══════════════════════════════╝
""")


def send_daily_summary():
    now       = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    if bot_state["last_summary_date"] == today_str:
        return
    if now.hour != DAILY_SUMMARY_HOUR_UTC:
        return

    try:
        current_balance = get_total_balance()
        # FIX: handle None (API error) — gunakan last known balance atau 0
        if current_balance is None:
            current_balance = _daily_limit_state.get("balance_open", 0) or 0
        else:
            refresh_pnl_snapshots(current_balance)
    except Exception:
        current_balance = 0

    # ── DAILY ──────────────────────────────────────────────────────────────
    day_wins   = bot_state.get("day_wins", 0)
    day_losses = bot_state.get("day_losses", 0)
    day_total  = day_wins + day_losses
    day_wr     = (day_wins / day_total * 100) if day_total > 0 else 0
    day_start  = bot_state.get("balance_day_start", 0)
    day_pnl    = current_balance - day_start if day_start > 0 else 0
    day_pnl_pct= (day_pnl / day_start * 100) if day_start > 0 else 0
    day_gross_win_pct  = day_pnl_pct if day_pnl >= 0 else 0.0
    day_gross_loss_pct = day_pnl_pct if day_pnl < 0 else 0.0
    day_net_pct= day_pnl_pct
    day_wr_bar_n = min(10, int(day_wr / 10))
    day_wr_bar   = "█" * day_wr_bar_n + "░" * (10 - day_wr_bar_n)
    day_label  = now.strftime("%Y-%m-%d")

    # ── MONTHLY ────────────────────────────────────────────────────────────
    mon_wins   = bot_state.get("month_wins", 0)
    mon_losses = bot_state.get("month_losses", 0)
    mon_total  = mon_wins + mon_losses
    mon_wr     = (mon_wins / mon_total * 100) if mon_total > 0 else 0
    mon_start  = bot_state.get("balance_month_start", 0)
    mon_pnl    = current_balance - mon_start if mon_start > 0 else 0
    mon_pnl_pct= (mon_pnl / mon_start * 100) if mon_start > 0 else 0
    mon_gross_win_pct  = mon_pnl_pct if mon_pnl >= 0 else 0.0
    mon_gross_loss_pct = mon_pnl_pct if mon_pnl < 0 else 0.0
    mon_net_pct= mon_pnl_pct
    mon_wr_bar_n = min(10, int(mon_wr / 10))
    mon_wr_bar   = "█" * mon_wr_bar_n + "░" * (10 - mon_wr_bar_n)
    mon_label  = now.strftime("%Y-%m")

    def _pct(v): return f"{'+'if v>=0 else ''}{v:.2f}%"
    def _em(v):  return "📈" if v >= 0 else "📉"

    msg = (
        f"📊 <b>PERFORMANCE SUMMARY</b>\n"
        f"{'─'*34}\n"
        f"📅 <b>DAILY ({day_label})</b>\n"
        f"  [{day_wr_bar}] WR: <b>{day_wr:.1f}%</b> ({day_wins}.0/{day_total}.0 poin)\n"
        f"  ✅ {day_wins} WIN ❌{day_losses} LOSS | Total:{day_total}\n"
        f"  {_em(day_gross_win_pct)} {_pct(day_gross_win_pct)}  "
        f"{'📉' if day_gross_loss_pct < 0 else '  '} {_pct(day_gross_loss_pct)}  "
        f"Net: <b>{_pct(day_net_pct)}</b>\n"
        f"  💰 Net PnL: <b>{_pct(day_net_pct)}</b>\n"
        f"{'─'*34}\n"
        f"🗓 <b>MONTHLY ({mon_label})</b>\n"
        f"  [{mon_wr_bar}] WR: <b>{mon_wr:.1f}%</b> ({mon_wins}.0/{mon_total}.0 poin)\n"
        f"  ✅ {mon_wins} WIN ❌{mon_losses} LOSS | Total:{mon_total}\n"
        f"  {_em(mon_gross_win_pct)} {_pct(mon_gross_win_pct)}  "
        f"{'📉' if mon_gross_loss_pct < 0 else '  '} {_pct(mon_gross_loss_pct)}  "
        f"Net: <b>{_pct(mon_net_pct)}</b>\n"
        f"  💰 Net PnL: <b>{_pct(mon_net_pct)}</b>\n"
        f"{'─'*34}\n"
        f"💵 Balance: <b>{current_balance:.2f} USDT</b> | ⚡ Bot aktif ✅"
    )

    send_telegram_raw(msg)
    bot_state["last_summary_date"] = today_str
    bot_state["signals_today"]     = 0


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20b — TELEGRAM COMMAND HANDLER
# ═══════════════════════════════════════════════════════════════════════════
#
#  Commands:
#   /pnl              — unrealized PnL semua posisi aktif
#   /status           — status bot + ringkasan posisi
#   /changemargin <SYMBOL> <ISOLATED|CROSSED>
#   /changelev <SYMBOL> <1-125>
#   /resumeorpause    — toggle pause/resume scanning & trading
#   /closeallposition — market close semua posisi aktif
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20c — TRADE LOGGER & PERFORMANCE ANALYTICS SYSTEM
# ═══════════════════════════════════════════════════════════════════════════
#
#  System:
#   - Setiap trade close dicatat ke trade_log.json (Railway Volume, persisten)
#   - Commands: /tradelog, /perfpair, /perftf, /tuning, /weeklysummary
#   - Auto weekly report setiap Senin UTC 00:00
#   - Parameter tuning suggestions berdasarkan data aktual
# ═══════════════════════════════════════════════════════════════════════════

_TRADE_LOG_FILE  = os.path.join(_get_data_dir(), "trade_log.json")
_trade_log_lock  = threading.Lock()
_last_weekly_report: str = ""   # format "YYYY-WNN" — cegah double send


def _load_trade_log() -> list:
    """Load trade log dari disk. Return list kosong jika belum ada."""
    try:
        if os.path.exists(_TRADE_LOG_FILE):
            with open(_TRADE_LOG_FILE, "r") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def _save_trade_log(log: list):
    """Simpan trade log ke disk (atomic write)."""
    try:
        tmp = _TRADE_LOG_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(log, f, indent=2)
        os.replace(tmp, _TRADE_LOG_FILE)
    except Exception as e:
        print(f"⚠️  Gagal simpan trade log: {e}")


# ══════════════════════════════════════════════════════════════════════════
#  VERSION STAMP — dicocokkan dengan Railway & GitHub
#  EDIT 3 BARIS INI SETIAP KALI UPLOAD VERSI BARU:
# ══════════════════════════════════════════════════════════════════════════
BOT_VERSION    = "v79-entry-fix"     # versi bot (ubah tiap rilis)
BUILD_DATE     = "2026-11-17"             # tanggal upload ke GitHub (YYYY-MM-DD)
GITHUB_ACCOUNT = "ISI_AKUN_GITHUB_KAMU"   # cadangan manual jika Railway tidak mengisi owner repo
# Sisanya otomatis dari env Railway (terisi jika deploy via integrasi GitHub).


def _vs_env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _bot_version_fields() -> dict:
    """Penanda versi untuk ditempel ke setiap baris trade_log."""
    return {
        "bot_version":       BOT_VERSION,
        "build_date":        BUILD_DATE,
        "git_sha":           _vs_env("RAILWAY_GIT_COMMIT_SHA")[:7],
        "git_branch":        _vs_env("RAILWAY_GIT_BRANCH"),
        "github_account":    _vs_env("RAILWAY_GIT_REPO_OWNER", GITHUB_ACCOUNT),
        "github_repo":       _vs_env("RAILWAY_GIT_REPO_NAME"),
        "railway_deploy_id": _vs_env("RAILWAY_DEPLOYMENT_ID")[:8],
        "railway_env":       _vs_env("RAILWAY_ENVIRONMENT_NAME"),
    }


def _bot_version_line_html() -> str:
    """Satu baris ringkas untuk Telegram (startup & /status)."""
    v = _bot_version_fields()
    repo = f"{v['github_account']}/{v['github_repo']}" if v["github_repo"] else v["github_account"]
    return (
        f"🏷 Versi : <b>{v['bot_version']}</b> | upload {v['build_date']}\n"
        f"🐙 GitHub: <code>{repo}</code> "
        f"({v['git_branch'] or '-'} @ {v['git_sha'] or '-'})\n"
        f"🚆 Railway: {v['railway_env'] or '-'} / deploy {v['railway_deploy_id'] or '-'}\n"
    )


def record_trade_close(
    symbol: str,
    direction: str,
    entry: float,
    close_price: float,
    sl: float,
    tp1: float,
    tp2: float,
    lot: float,
    pnl_usdt: float,
    close_type: str,      # "TP1" | "TP2" | "SL" | "BE" | "MANUAL" | "TP2_TIMEOUT"
    mode_label: str,      # "INTRADAY" | "SCALPING" | "LTF_30M" | dll
    entry_tf: str,        # "15m" | "30m" | "1h" | "4h"
    score: float = 0,
    rr_actual: float = 0,
    score_bd: dict = None,   # breakdown komponen skor (bos/liquidity/zone/dst)
    # ── Konteks market saat entry + price-path selama posisi terbuka ────────
    # Untuk analisis kausal: "market gerak begini → SL kekecilan/posisi salah
    # arah" — bukan cuma tahu hasil menang/kalah tapi KENAPA.
    atr_pct: float = 0,          # ATR% harga saat entry (ukuran volatilitas)
    sl_atr_mult: float = 0,      # jarak SL dalam satuan ATR (SL kekecilan relatif?)
    vol_rat: float = 0,          # volume candle entry vs rata-rata 20 candle
    mkt_regime: str = "?",       # v78: BULL_REGIME / BEAR_REGIME / MIXED saat sinyal dibuat
    alt_bull_pct: float = -1,    # v78: % altcoin sampel yang bullish (1h)
    alt_bear_pct: float = -1,    # v78: % altcoin sampel yang bearish (1h)
    htf_bias: str = "?",         # bias HTF saat sinyal dibuat
    bos_strength: str = "?",     # STRONG/WEAK/NONE
    engulf_break: str = "?",     # YES/NO/OFF — breakout ditembus candle engulfing + FVG?
    engulf_entry: str = "?",     # YES/NO/OFF — candle konfirmasi entry engulfing searah?
    engulf_note: str = "",       # alasan singkat kalau engulf_break = NO
    fvg_retest: str = "?",       # YES/NO/OFF — retest FVG close di sisi searah dari tengah FVG?
    fvg_retest_note: str = "",   # alasan singkat kalau fvg_retest = NO
    stoch_div: str = "?",        # YES/NO/OFF — ada divergence stochastic searah trade?
    stoch_weak: str = "?",       # YES/NO/OFF — stochastic melemah (dari OB utk SHORT / OS utk LONG)?
    stoch_note: str = "",        # nilai K/D saat entry + alasan singkat
    sweep_strength: str = "?",   # STRONG/RELAXED/NONE
    sl_source: str = "?",        # OB/FVG/SWING — SL dibentuk dari struktur apa
    rsi_entry: float = 0,        # RSI saat entry
    mae_pct: float = 0,          # max adverse excursion selama posisi terbuka
    mfe_pct: float = 0,          # max favorable excursion selama posisi terbuka
    open_time: str = "",         # ISO timestamp saat posisi dibuka
    btc_bias: str = "?",         # bias BTC HTF saat entry (BULLISH/BEARISH/RANGING)
    btcd_trend: str = "?",       # trend BTC Dominance HTF saat entry
    btc_ltf_dir: str = "?",      # arah BTC di TF pendek saat entry
    btcd_ltf_dir: str = "?",     # arah BTCD di TF pendek saat entry
    # ── Snapshot BTC/BTCD SAAT CLOSE (bukan saat entry) — untuk evaluasi
    # apakah SL dipicu/didahului oleh reversal BTC atau BTCD selagi posisi
    # berjalan. Diisi via _get_btc_btcd_snapshot() oleh caller.
    btc_bias_at_close: str = "?",
    btcd_trend_at_close: str = "?",
    btc_ltf_dir_at_close: str = "?",
    btcd_ltf_dir_at_close: str = "?",
):
    """
    Catat satu trade ke trade_log.json.
    Dipanggil dari notify_sl_hit, notify_tp2_hit, manage_trailing, sync_closed_positions.
    """
    # ── FIX KRITIS: guard trade "hantu" (lot<=0) ──────────────────────────────
    # Root cause dari ratusan entry trade_log.json dengan lot=0 yang mencemari
    # winrate stats (analisa 2026-07-17: 614/634 baris log = lot 0, semuanya
    # tercatat SL walau tidak pernah ada posisi/order real). lot<=0 berarti
    # tidak ada quantity yang benar2 dieksekusi di Binance — ini TIDAK BOLEH
    # dianggap trade yang "closed SL/TP", karena akan menghancurkan statistik
    # winrate walau strategi aslinya baik-baik saja.
    # Traceback di-print supaya kalau kejadian lagi, log Railway langsung
    # nunjukin fungsi/baris pemanggil persis (root cause belum 100% terpin
    # secara statis — file terlalu besar untuk ditelusuri manual sepenuhnya).
    if not lot or lot <= 0:
        import traceback
        print(
            f"  🚨 [record_trade_close] DITOLAK — {symbol} {direction} {close_type} "
            f"punya lot={lot} (<=0), trade ini TIDAK di-log ke trade_log.json.\n"
            f"     entry={entry} close={close_price} sl={sl} mode={mode_label} "
            f"entry_tf={entry_tf} score={score}\n"
            f"     Call stack (buat debugging root cause):"
        )
        traceback.print_stack(limit=8)
        return

    sl_dist  = abs(entry - sl)
    tp1_dist = abs(tp1 - entry)
    rr_plan  = (tp1_dist / sl_dist) if sl_dist > 0 else 0

    close_time = datetime.now(timezone.utc)

    _duration_sec = 0
    if open_time:
        try:
            _open_dt = datetime.fromisoformat(open_time)
            _duration_sec = round((close_time - _open_dt).total_seconds())
        except Exception:
            _duration_sec = 0

    record = {
        "ts":          close_time.isoformat(),
        "symbol":      symbol,
        "direction":   direction,
        "entry":       entry,
        "close_price": close_price,
        "sl":          sl,
        "tp1":         tp1,
        "tp2":         tp2,
        "lot":         lot,
        "pnl_usdt":    round(pnl_usdt, 4),
        "close_type":  close_type,
        "mode":        mode_label,
        "entry_tf":    entry_tf,
        "score":       score,
        "rr_plan":     round(rr_plan, 2),
        "rr_actual":   round(rr_actual, 2),
        "is_win":      pnl_usdt > 0,
        "score_bd":    score_bd if isinstance(score_bd, dict) else {},
        # ── Konteks kausal ────────────────────────────────────────────────
        "atr_pct":        atr_pct,
        "sl_atr_mult":    sl_atr_mult,
        "vol_rat":        vol_rat,
        "mkt_regime":     mkt_regime,
        "alt_bull_pct":   alt_bull_pct,
        "alt_bear_pct":   alt_bear_pct,
        "htf_bias":       htf_bias,
        "bos_strength":   bos_strength,
        "engulf_break":   engulf_break,
        "engulf_entry":   engulf_entry,
        "engulf_note":    engulf_note,
        "fvg_retest":     fvg_retest,
        "fvg_retest_note": fvg_retest_note,
        "stoch_div":      stoch_div,
        "stoch_weak":     stoch_weak,
        "stoch_note":     stoch_note,
        "sweep_strength": sweep_strength,
        "sl_source":      sl_source,
        "rsi_entry":      rsi_entry,
        "mae_pct":        mae_pct,
        "mfe_pct":        mfe_pct,
        "duration_sec":   _duration_sec,
        "btc_bias":       btc_bias,
        "btcd_trend":     btcd_trend,
        "btc_ltf_dir":    btc_ltf_dir,
        "btcd_ltf_dir":   btcd_ltf_dir,
        # Snapshot saat close — lihat komentar parameter di atas
        "btc_bias_at_close":     btc_bias_at_close,
        "btcd_trend_at_close":   btcd_trend_at_close,
        "btc_ltf_dir_at_close":  btc_ltf_dir_at_close,
        "btcd_ltf_dir_at_close": btcd_ltf_dir_at_close,
        # ── Penanda versi bot (Railway/GitHub) ─────────────────────────────
        **_bot_version_fields(),
    }

    with _trade_log_lock:
        log = _load_trade_log()
        # ── Safety net anti-duplikat: posisi yang sama (symbol+arah+entry+open_time)
        # tidak boleh tercatat close dua kali. Kalau open_time kosong, fallback ke
        # jendela waktu 10 menit.
        for _prev in log[-30:]:
            if (_prev.get("symbol") == symbol and _prev.get("direction") == direction
                    and _prev.get("entry") == entry):
                _same_pos = False
                if open_time and _prev.get("duration_sec") is not None:
                    try:
                        _prev_close = datetime.fromisoformat(_prev["ts"])
                        _prev_open = _prev_close - timedelta(seconds=_prev.get("duration_sec", 0))
                        _cur_open = datetime.fromisoformat(open_time)
                        _same_pos = abs((_prev_open - _cur_open).total_seconds()) < 5
                    except Exception:
                        _same_pos = False
                else:
                    try:
                        _same_pos = (close_time - datetime.fromisoformat(_prev["ts"])).total_seconds() < 600
                    except Exception:
                        _same_pos = False
                if _same_pos:
                    print(f"  🛡️  [record_trade_close] DUPLIKAT {symbol} {close_type} "
                          f"(sudah tercatat {_prev.get('close_type')}) — tidak di-log lagi.")
                    return
        log.append(record)
        # Simpan max 2000 trade terakhir agar file tidak tumbuh tak terbatas
        if len(log) > 2000:
            log = log[-2000:]
        _save_trade_log(log)

    print(f"  📝 Trade logged: {symbol} {direction} {close_type} PnL={pnl_usdt:+.2f}")


def cmd_dedupelog(parts: list = None):
    """
    /dedupelog — bersihkan baris dobel di trade_log.json (1 posisi tercatat
    TP2 + BE). Per grup (symbol+arah+entry+lot, close berdekatan <=10 menit)
    yang dipertahankan 1 baris: prioritas TP2 > TP1 > SL > MANUAL > BE.
    """
    prio = {"TP2": 0, "TP1": 1, "SL": 2, "MANUAL": 3, "BE": 4}
    with _trade_log_lock:
        log = _load_trade_log()
        if not log:
            send_telegram_raw("📭 Trade log kosong.")
            return
        keep = []
        removed = 0
        for t in log:
            dup_idx = None
            for i in range(len(keep) - 1, max(len(keep) - 31, -1), -1):
                k = keep[i]
                if (k.get("symbol") == t.get("symbol") and k.get("direction") == t.get("direction")
                        and k.get("entry") == t.get("entry") and k.get("lot") == t.get("lot")):
                    try:
                        dt = abs((datetime.fromisoformat(t["ts"]) - datetime.fromisoformat(k["ts"])).total_seconds())
                    except Exception:
                        continue
                    if dt <= 600:
                        dup_idx = i
                        break
            if dup_idx is None:
                keep.append(t)
            else:
                removed += 1
                if prio.get(t.get("close_type"), 9) < prio.get(keep[dup_idx].get("close_type"), 9):
                    keep[dup_idx] = t
        if removed:
            _save_trade_log(keep)
    send_telegram_raw(f"🧹 Dedupe selesai: {removed} baris dobel dihapus, sisa {len(keep) if removed else len(log)} trade.")


def _analyze_trade_log(log: list, days: int = 30) -> dict:
    """
    Hitung statistik dari trade log.
    Return dict berisi semua metrik yang dipakai di report & tuning.
    """
    if not log:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    recent = []
    for t in log:
        try:
            ts = datetime.fromisoformat(t["ts"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                recent.append(t)
        except Exception:
            pass

    if not recent:
        return {}

    total   = len(recent)
    wins    = [t for t in recent if t["is_win"]]
    losses  = [t for t in recent if not t["is_win"]]
    win_cnt = len(wins)
    los_cnt = len(losses)
    wr      = win_cnt / total * 100 if total else 0

    pnl_list      = [t["pnl_usdt"] for t in recent]
    total_pnl     = sum(pnl_list)
    avg_win       = sum(t["pnl_usdt"] for t in wins)   / max(win_cnt, 1)
    avg_loss      = sum(t["pnl_usdt"] for t in losses)  / max(los_cnt, 1)
    profit_factor = abs(sum(t["pnl_usdt"] for t in wins)) / max(abs(sum(t["pnl_usdt"] for t in losses)), 0.01)
    expectancy    = (wr/100 * avg_win) + ((1 - wr/100) * avg_loss)

    # Per-pair, per-tf, per-mode, per-direction stats
    def _agg(key):
        d = {}
        for t in recent:
            k = t.get(key, "?")
            if k not in d:
                d[k] = {"wins": 0, "losses": 0, "pnl": 0.0}
            d[k]["wins" if t["is_win"] else "losses"] += 1
            d[k]["pnl"] += t["pnl_usdt"]
        return d

    by_pair  = _agg("symbol")
    by_tf    = _agg("entry_tf")
    by_mode  = _agg("mode")
    by_dir   = _agg("direction")

    by_close = {}
    for t in recent:
        ct = t.get("close_type", "?")
        by_close[ct] = by_close.get(ct, 0) + 1

    # Max drawdown (running)
    running, peak, max_dd = 0.0, 0.0, 0.0
    for p in pnl_list:
        running += p
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)

    rr_actuals    = [t.get("rr_actual", 0) for t in wins if t.get("rr_actual", 0) > 0]
    avg_rr_win    = sum(rr_actuals) / len(rr_actuals) if rr_actuals else 0
    scores_win    = [t.get("score", 0) for t in wins   if t.get("score", 0) > 0]
    scores_los    = [t.get("score", 0) for t in losses if t.get("score", 0) > 0]
    avg_score_win = sum(scores_win) / len(scores_win) if scores_win else 0
    avg_score_los = sum(scores_los) / len(scores_los) if scores_los else 0

    return {
        "total": total, "wins": win_cnt, "losses": los_cnt, "wr": wr,
        "total_pnl": total_pnl, "avg_win": avg_win, "avg_loss": avg_loss,
        "profit_factor": profit_factor, "expectancy": expectancy,
        "max_dd": max_dd, "avg_rr_win": avg_rr_win,
        "avg_score_win": avg_score_win, "avg_score_los": avg_score_los,
        "by_pair": by_pair, "by_tf": by_tf, "by_mode": by_mode,
        "by_dir": by_dir, "by_close": by_close, "days": days,
    }


_TRADE_LOG_CSV_FIELDS = [
    "ts", "symbol", "direction", "entry", "close_price", "sl", "tp1", "tp2",
    "lot", "pnl_usdt", "close_type", "mode", "entry_tf", "score",
    "rr_plan", "rr_actual", "is_win", "score_bd",
    # ── Konteks kausal (2026-09-01) ──────────────────────────────────────
    "atr_pct", "sl_atr_mult", "vol_rat", "htf_bias", "bos_strength",
    "engulf_break", "engulf_entry", "engulf_note", "fvg_retest", "fvg_retest_note",
    "stoch_div", "stoch_weak", "stoch_note",
    "sweep_strength", "sl_source", "rsi_entry", "mae_pct", "mfe_pct",
    "duration_sec", "btc_bias", "btcd_trend", "btc_ltf_dir", "btcd_ltf_dir",
    "mkt_regime", "alt_bull_pct", "alt_bear_pct",
    # ── Snapshot BTC/BTCD saat CLOSE, bukan saat entry (2026-09-07) ───────
    "btc_bias_at_close", "btcd_trend_at_close",
    "btc_ltf_dir_at_close", "btcd_ltf_dir_at_close",
    # ── Penanda versi bot (baris lama = kosong) ────────────────────────────
    "bot_version", "build_date", "git_sha", "git_branch",
    "github_account", "github_repo", "railway_deploy_id", "railway_env",
]


def _export_trade_log_csv(days: int = None) -> tuple[bytes, int]:
    """
    Konversi trade_log.json → CSV bytes, siap dikirim/didownload.
    days=None → semua data. days=N → hanya N hari terakhir.
    Return (csv_bytes, jumlah_baris).
    """
    log = _load_trade_log()

    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        rows = []
        for t in log:
            try:
                ts = datetime.fromisoformat(t["ts"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    rows.append(t)
            except Exception:
                pass
    else:
        rows = log

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_TRADE_LOG_CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for t in rows:
        writer.writerow(t)

    return buf.getvalue().encode("utf-8"), len(rows)


def _score_bucket(score: float) -> str:
    """Kelompokkan score jadi bucket biar bisa dibandingkan winrate-nya."""
    if not score or score <= 0: return "?"
    if score < 40:               return "<40"
    if score < 50:                return "40-49"
    if score < 55:                return "50-54"
    if score < 60:                return "55-59"
    if score < 70:                return "60-69"
    if score < 80:                return "70-79"
    return "80+"


_SCORE_BUCKET_ORDER = ["<40", "40-49", "50-54", "55-59", "60-69", "70-79", "80+", "?"]


def cmd_market(parts: list):
    """
    /market — tampilkan market regime SEKARANG: berapa % altcoin sampel yang
    bullish/bearish (1h) + apakah BTC mengonfirmasi. Dipakai untuk melihat arah
    dominan pasar sebelum memilih SHORT atau LONG.
    """
    try:
        _bb = get_btc_bias()
        try:
            _bd = get_btcd_bias()
        except Exception:
            _bd = "?"
        reg = get_market_regime_signalonly(_bb, force_refresh=True)
        r = reg.get("regime", "MIXED")
        arti = {
            "BEAR_REGIME": "Mayoritas altcoin turun & BTC turun → pasar condong ke SHORT.",
            "BULL_REGIME": "Mayoritas altcoin naik & BTC naik → pasar condong ke LONG.",
            "MIXED":       "Altcoin tidak searah atau BTC tidak mengonfirmasi → tidak ada arah dominan.",
        }.get(r, "")
        msg = (
            "🌐 <b>MARKET REGIME</b>\n\n"
            f"{reg.get('detail', '')}\n\n"
            f"BTC bias: <b>{_bb}</b> | BTC.D: <b>{_bd}</b>\n"
            f"Sampel: {reg.get('sample_size', 0)} altcoin ({REGIME_TF})\n\n"
            f"➡️ <b>{r}</b>\n{arti}\n\n"
            f"Gate otomatis: {'AKTIF' if EDGE_REGIME_GATE_ON else 'MATI (hanya dicatat ke log trade)'}"
        )
        send_telegram_raw(msg)
    except Exception as e:
        send_telegram_raw(f"⚠️ /market gagal: {e}")


def cmd_scorewr(parts: list):
    """
    /scorewr [hari|all]
    Breakdown WINRATE per score bucket dari trade_log.json — supaya keputusan
    naik/turunin score gate berdasar DATA ASLI, bukan cuma feeling "kayaknya
    gak ngaruh". Default 30 hari. /scorewr 90 → 90 hari. /scorewr all → semua data.

    Cara baca hasilnya:
      - Kalau WR bucket score TINGGI jelas lebih baik dari bucket score RENDAH
        (misal 80+ = 65% WR, <40 = 35% WR) → score gate emang predictive,
        JANGAN diturunin — turunin gate = sengaja masukin setup lemah.
      - Kalau WR semua bucket MIRIP-MIRIP / gak ada tren naik seiring score
        → scoring formula ini lemah predictive power-nya di kondisi market
        sekarang, dan masuk akal buat direvisi/diturunin gate-nya.
      - Perhatikan JUMLAH TRADE (n) per bucket juga — kalau n kecil (<10),
        WR bucket itu belum bisa dipercaya, masih random noise.
    """
    log = _load_trade_log()
    if not log:
        send_telegram_raw("📭 Trade log masih kosong — belum ada data buat dianalisa.")
        return

    days_arg = parts[1] if len(parts) > 1 else "30"
    if days_arg.lower() == "all":
        recent = log
        days_label = "semua data"
    else:
        try:
            days = int(days_arg)
        except ValueError:
            days = 30
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent = []
        for t in log:
            try:
                ts = datetime.fromisoformat(t["ts"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    recent.append(t)
            except Exception:
                pass
        days_label = f"{days} hari terakhir"

    if not recent:
        send_telegram_raw(f"📭 Tidak ada trade dalam rentang {days_label}.")
        return

    buckets: dict = {}
    for t in recent:
        b = _score_bucket(t.get("score", 0))
        d = buckets.setdefault(b, {"wins": 0, "losses": 0, "pnl": 0.0})
        d["wins" if t.get("is_win") else "losses"] += 1
        d["pnl"] += t.get("pnl_usdt", 0)

    lines = [f"📊 <b>Winrate per Score Bucket</b> ({days_label})", "─" * 34]
    total_n = len(recent)
    any_low_n = False
    for b in _SCORE_BUCKET_ORDER:
        if b not in buckets:
            continue
        d  = buckets[b]
        n  = d["wins"] + d["losses"]
        wr = (d["wins"] / n * 100) if n else 0
        if n < 10:
            any_low_n = True
        flag = " ⚠️n kecil" if n < 10 else ""
        lines.append(
            f"Score {b:>6s} │ n={n:<3d} │ WR {wr:5.1f}% │ PnL {d['pnl']:+7.2f} USDT{flag}"
        )
    lines.append("─" * 34)
    lines.append(f"Total trade dianalisa: <b>{total_n}</b>")
    if any_low_n:
        lines.append("⚠️ Ada bucket dengan n<10 — WR-nya belum representatif, jangan diambil kesimpulan dulu dari bucket itu.")
    lines.append(
        "\nℹ️ Kalau WR naik jelas seiring score (bucket tinggi > bucket rendah) → "
        "gate ini beneran predictive, jangan diturunin. Kalau WR-nya flat/acak "
        "di semua bucket → gate score ini lemah, layak direvisi."
    )
    send_telegram_raw("\n".join(lines))


# ── Komponen skor yang mau dipecah per-value ──────────────────────────────
# key = nama field di score_bd (lihat compute_score()), label = tampilan.
# Nilai float mentah (poin) di-bucket otomatis; kalau mau breakdown per
# kategori string (STRONG/WEAK/NONE dsb), field itu tidak disimpan di
# score_bd (score_bd isinya poin numerik hasil scoring, bukan label
# kategorinya) — jadi breakdown di sini berbasis POIN, bukan label mentah.
_SCORE_BD_COMPONENTS = [
    ("htf",          "HTF Alignment"),
    ("bos",          "BOS / Struktur"),
    ("momentum",     "Momentum"),
    ("liquidity",    "Liquidity Sweep"),
    ("entry_zone",   "OB/FVG Entry Zone"),
    ("displacement", "Displacement Candle"),
    ("volume",       "Volume Ratio"),
    ("session",      "Session"),
    ("rsi",          "RSI"),
    ("regime",       "Regime ADX"),
    ("macro",        "Macro (BTC/BTCD)"),
    ("ema",          "EMA"),
    ("ref_tf",       "Ref TF Alignment"),
]


def _bd_value_bucket(val) -> str:
    """
    Kelompokkan nilai poin komponen jadi label kasar: negatif / nol / positif-lemah
    / positif-kuat. Threshold kasar karena skala poin beda-beda per komponen —
    tujuannya cuma bedain 'komponen ini berkontribusi negatif/netral/positif'.
    """
    try:
        v = float(val)
    except (TypeError, ValueError):
        return "?"
    if v < 0:
        return "negatif"
    if v == 0:
        return "nol"
    if v <= 5:
        return "positif-lemah"
    return "positif-kuat"


def cmd_scorebd(parts: list):
    """
    /scorebd [hari|all]
    Breakdown WINRATE per KOMPONEN individual skor (BOS, Liquidity, Zone,
    Volume, dst) — beda dari /scorewr yang breakdown per TOTAL score.

    Tujuan: /scorewr jawab "score berapa yang WR-nya tinggi", /scorebd jawab
    "komponen mana yang beneran nyumbang ke WR tinggi" — supaya bisa lihat
    apakah misal Volume tinggi konsisten menang, atau malah nggak ngaruh
    sama sekali (dan bisa dikurangi bobotnya di scoring tanpa nurunin WR).

    Catatan penting: data score_bd baru mulai ke-log dari update fix ini
    (sebelumnya trade LOW_TF tidak menyimpan breakdown komponen). Jadi kalau
    hasil /scorebd n=0 di semua komponen, itu karena trade_log lama belum
    punya data ini — tunggu beberapa trade baru dulu setelah fix ter-deploy.

    /scorebd 90 → 90 hari. /scorebd all → semua data.
    """
    log = _load_trade_log()
    if not log:
        send_telegram_raw("📭 Trade log masih kosong — belum ada data buat dianalisa.")
        return

    days_arg = parts[1] if len(parts) > 1 else "30"
    if days_arg.lower() == "all":
        recent = log
        days_label = "semua data"
    else:
        try:
            days = int(days_arg)
        except ValueError:
            days = 30
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        recent = []
        for t in log:
            try:
                ts = datetime.fromisoformat(t["ts"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    recent.append(t)
            except Exception:
                pass
        days_label = f"{days} hari terakhir"

    if not recent:
        send_telegram_raw(f"📭 Tidak ada trade dalam rentang {days_label}.")
        return

    # Cuma hitung trade yang punya score_bd terisi (trade lama sebelum fix
    # ini akan punya score_bd={} — dikeluarkan dari analisa, bukan dianggap 0).
    with_bd = [t for t in recent if t.get("score_bd")]
    n_no_bd = len(recent) - len(with_bd)

    if not with_bd:
        send_telegram_raw(
            f"📭 Tidak ada trade dengan data breakdown skor di {days_label} "
            f"({n_no_bd} trade tanpa score_bd — kemungkinan dari sebelum fix "
            f"ini ter-deploy). Tunggu beberapa trade baru dulu."
        )
        return

    lines = [f"📊 <b>Winrate per Komponen Skor</b> ({days_label})", "─" * 34]
    if n_no_bd:
        lines.append(f"ℹ️ {n_no_bd} trade lama dilewati (belum punya data breakdown).\n")

    any_low_n = False
    for key, label in _SCORE_BD_COMPONENTS:
        buckets: dict = {}
        for t in with_bd:
            bd = t.get("score_bd", {})
            if key not in bd:
                continue
            b = _bd_value_bucket(bd[key])
            d = buckets.setdefault(b, {"wins": 0, "losses": 0, "pnl": 0.0})
            d["wins" if t.get("is_win") else "losses"] += 1
            d["pnl"] += t.get("pnl_usdt", 0)

        n_component = sum(d["wins"] + d["losses"] for d in buckets.values())
        if n_component == 0:
            continue

        lines.append(f"\n<b>{label}</b>")
        for b in ("negatif", "nol", "positif-lemah", "positif-kuat", "?"):
            if b not in buckets:
                continue
            d  = buckets[b]
            n  = d["wins"] + d["losses"]
            wr = (d["wins"] / n * 100) if n else 0
            if n < 10:
                any_low_n = True
            flag = " ⚠️n kecil" if n < 10 else ""
            lines.append(
                f"  {b:<14s} │ n={n:<3d} │ WR {wr:5.1f}% │ PnL {d['pnl']:+7.2f}{flag}"
            )

    lines.append("\n" + "─" * 34)
    lines.append(f"Total trade dianalisa: <b>{len(with_bd)}</b>")
    if any_low_n:
        lines.append("⚠️ Ada bucket dengan n<10 — WR-nya belum representatif.")
    lines.append(
        "\nℹ️ Komponen yang WR-nya konsisten lebih tinggi di 'positif-kuat' "
        "dibanding 'negatif/nol' → beneran predictive, pertahankan bobotnya. "
        "Komponen yang WR-nya flat di semua bucket → kurang berpengaruh, "
        "kandidat buat dikurangi bobotnya di compute_score()."
    )
    send_telegram_raw("\n".join(lines))


def cmd_resetlog(parts: list):
    """
    /resetlog
    Arsipkan trade_log.json yang ada sekarang (TIDAK dihapus, cuma di-rename
    dengan timestamp) lalu mulai trade_log.json baru dari kosong.

    Berguna kalau trade_log.json kecemar data lama — misal dari versi bot
    sebelumnya yang masih share Railway Volume yang sama (trade_log.json
    persisten di volume, TIDAK ikut ke-reset waktu redeploy/ganti bot).

    /resetlog confirm   → wajib ada kata "confirm" biar gak ke-trigger salah pencet.
    """
    if len(parts) < 2 or parts[1].lower() != "confirm":
        n_existing = len(_load_trade_log())
        send_telegram_raw(
            f"⚠️ <b>/resetlog — Konfirmasi Diperlukan</b>\n"
            f"{'─'*34}\n"
            f"Trade log saat ini: <b>{n_existing} baris</b>.\n"
            f"Aksi ini akan:\n"
            f"  1. Arsipkan trade_log.json sekarang (TIDAK dihapus, cuma di-rename)\n"
            f"  2. Mulai trade_log.json baru dari kosong\n"
            f"{'─'*34}\n"
            f"Ketik <code>/resetlog confirm</code> untuk lanjut."
        )
        return

    log = _load_trade_log()
    n_archived = len(log)

    archive_name = f"trade_log_archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    archive_path = os.path.join(_get_data_dir(), archive_name)

    try:
        with _trade_log_lock:
            if os.path.exists(_TRADE_LOG_FILE):
                os.replace(_TRADE_LOG_FILE, archive_path)
            # Mulai file baru kosong
            _save_trade_log([])
    except Exception as e:
        send_telegram_raw(f"❌ <b>/resetlog gagal:</b> <code>{e}</code>")
        print(f"⚠️ /resetlog error: {e}")
        return

    send_telegram_raw(
        f"✅ <b>Trade Log Di-reset</b>\n"
        f"{'─'*34}\n"
        f"📦 {n_archived} baris lama diarsipkan ke:\n<code>{archive_name}</code>\n"
        f"🆕 trade_log.json sekarang kosong — mulai tracking dari nol.\n"
        f"{'─'*34}\n"
        f"ℹ️ File arsip masih ada di Railway Volume kalau perlu dicek lagi nanti.\n"
        f"Gunakan <code>/exportlog</code> setelah beberapa hari untuk cek data baru."
    )
    print(f"⚙️ /resetlog: {n_archived} baris diarsipkan ke {archive_name}, trade_log.json direset kosong")


def cmd_clearcooldown(parts: list):
    """
    /clearcooldown <PAIR>   → hapus cooldown untuk 1 pair (contoh: /clearcooldown ATOMUSDT)
    /clearcooldown all      → hapus SEMUA cooldown yang sedang aktif

    ROOT CAUSE yang di-fix command ini: _cooldown_map di-persist ke
    bot_state.json di Railway Volume, sehingga BERTAHAN lintas redeploy/
    restart — beda dari kode main.py yang berubah tiap kamu ganti file.
    Akibatnya pair bisa "nyangkut" cooldown dari attempt LAMA (misal LIMIT
    order yang sempat terkirim/fill lalu ke-cancel/close cepat karena bug
    lain) padahal SEKARANG tidak ada posisi/history trade sama sekali untuk
    pair itu — dan pair itu jadi terus di-skip "Cooldown Xj lagi" walau
    sebenarnya bug penyebabnya sudah diperbaiki.

    Command ini TIDAK menghapus volume atau data lain (trade_log, state
    posisi aktif) — hanya cooldown map. Aman dipakai kapan saja.
    """
    global _cooldown_map

    if len(parts) < 2:
        n = len(_cooldown_map)
        send_telegram_raw(
            f"ℹ️ <b>/clearcooldown — Cara Pakai</b>\n"
            f"{'─'*34}\n"
            f"Cooldown aktif sekarang: <b>{n} entri</b>.\n\n"
            f"<code>/clearcooldown ATOMUSDT</code> → hapus cooldown pair ini saja\n"
            f"<code>/clearcooldown all</code> → hapus SEMUA cooldown\n"
        )
        return

    target = parts[1].strip().upper()

    if target == "ALL":
        n_removed = len(_cooldown_map)
        _cooldown_map = {}
        save_state()
        send_telegram_raw(
            f"✅ <b>Semua Cooldown Dihapus</b>\n"
            f"{'─'*34}\n"
            f"{n_removed} entri cooldown dibersihkan dari state.\n"
            f"Semua pair sekarang bebas cooldown sinyal (kecuali cooldown "
            f"post-close/blacklist yang baru terbentuk lagi dari trade nyata)."
        )
        print(f"⚙️ /clearcooldown all: {n_removed} entri dihapus")
        return

    # Normalisasi: terima "ATOM", "ATOMUSDT", atau "ATOM/USDT"
    _base = target.replace("/USDT", "").replace("USDT", "").replace("/", "")
    _candidates = [target, f"{_base}USDT", f"{_base}/USDT"]

    removed_keys = [k for k in list(_cooldown_map.keys())
                    if any(k.startswith(f"{c}|") for c in _candidates)]

    if not removed_keys:
        send_telegram_raw(
            f"📭 Tidak ada cooldown aktif untuk <b>{target}</b>.\n"
            f"(Cek ejaan — coba <code>/clearcooldown all</code> untuk lihat semua "
            f"kalau tidak yakin format pair-nya.)"
        )
        return

    for k in removed_keys:
        del _cooldown_map[k]
    save_state()

    send_telegram_raw(
        f"✅ <b>Cooldown {target} Dihapus</b>\n"
        f"{'─'*34}\n"
        f"Entri dihapus: <code>{', '.join(removed_keys)}</code>\n"
        f"Pair ini sekarang bebas cooldown sinyal, sinyal baru bisa langsung diproses."
    )
    print(f"⚙️ /clearcooldown {target}: dihapus {removed_keys}")


def cmd_exportlog(parts: list):
    """
    /exportlog          → export semua trade history (trade_log.json) jadi CSV
    /exportlog 7        → hanya 7 hari terakhir
    /exportlog 30       → hanya 30 hari terakhir
    File CSV ini bisa didownload lalu diupload ke Claude untuk dievaluasi
    (win rate, pair mana yang sering rugi, dsb).
    """
    days = None
    if len(parts) > 1:
        try:
            days = int(parts[1])
        except ValueError:
            send_telegram_raw("⚠️ Format salah. Contoh: <code>/exportlog</code> atau <code>/exportlog 30</code>")
            return

    csv_bytes, n_rows = _export_trade_log_csv(days=days)

    if n_rows == 0:
        send_telegram_raw("📭 Belum ada trade yang tercatat di log — belum ada yang bisa diexport.")
        return

    label = f"{days} hari terakhir" if days else "semua waktu"
    fname_part = f"{days}d" if days else "all"
    filename = f"trade_log_{fname_part}_{datetime.now().strftime('%Y%m%d')}.csv"

    ok = send_telegram_document(
        csv_bytes,
        filename,
        caption=f"📊 <b>Trade Log Export</b>\nPeriode: {label}\nTotal trade: {n_rows}\n\nFile ini bisa langsung diupload ke Claude untuk dievaluasi.",
    )
    if not ok:
        send_telegram_raw("❌ Gagal mengirim file export. Coba lagi.")


def _fmt_wr_bar(wr: float, width: int = 8) -> str:
    filled = min(width, int(wr / 100 * width))
    return "█" * filled + "░" * (width - filled)


# ── Sistem poin untuk recap "SEMUA WAKTU" ────────────────────────────────
# TP2 = +1 poin, TP1 = +1 poin, SL/lainnya = 0 poin (menghitung WIN/LOSS
# berbasis close_type, BUKAN cuma pnl_usdt>0, biar konsisten sama gaya
# recap manual yang biasa dipakai user).
_RECAP_POINT_MAP = {"TP2": 1, "TP1": 1}


def _recap_is_win(t: dict) -> bool:
    """WIN kalau close_type TP1/TP2 (poin), selain itu (SL/BE/MANUAL/dll) LOSS."""
    return t.get("close_type", "") in _RECAP_POINT_MAP


def _recap_pnl_pct(t: dict) -> float:
    """PnL% per trade = pnl_usdt / notional (entry*lot) * 100 (tanpa leverage)."""
    try:
        entry = float(t.get("entry") or 0)
        lot   = float(t.get("lot") or 0)
        notional = entry * lot
        if notional > 0:
            return t.get("pnl_usdt", 0) / notional * 100
    except Exception:
        pass
    return 0.0


def _recap_block(rows: list, label: str) -> str:
    """Satu blok statistik recap (poin, WIN/LOSS, PnL%) untuk kumpulan trade."""
    if not rows:
        return f"📊 <b>{label}</b>\nBelum ada trade.\n"
    win_pts   = sum(_RECAP_POINT_MAP.get(t.get("close_type", ""), 0) for t in rows)
    total_pts = len(rows)
    wr_pts    = (win_pts / total_pts * 100) if total_pts else 0
    n_win     = sum(1 for t in rows if _recap_is_win(t))
    n_loss    = total_pts - n_win

    pnl_pcts       = [_recap_pnl_pct(t) for t in rows]
    total_win_pct  = sum(p for p in pnl_pcts if p > 0)
    total_loss_pct = sum(p for p in pnl_pcts if p < 0)
    net_pct        = total_win_pct + total_loss_pct
    avg_win_pct    = (total_win_pct / n_win) if n_win else 0
    avg_loss_pct   = (total_loss_pct / n_loss) if n_loss else 0
    net_em         = "🟢" if net_pct >= 0 else "🔴"
    sgn            = "+" if net_pct >= 0 else ""

    # ── Breakdown jujur: TP2 / BE / SL + expectancy (BE bukan "win" sungguhan) ──
    _n_tp2 = sum(1 for t in rows if t.get("close_type") == "TP2")
    _n_be  = sum(1 for t in rows if t.get("close_type") == "BE")
    _n_sl  = sum(1 for t in rows if t.get("close_type") == "SL")
    _n_oth = total_pts - _n_tp2 - _n_be - _n_sl
    _pnls  = []
    for t in rows:
        try:
            _pnls.append(float(t.get("pnl_usdt", 0) or 0))
        except Exception:
            _pnls.append(0.0)
    _gp = sum(p for p in _pnls if p > 0)
    _gl = -sum(p for p in _pnls if p < 0)
    _pf_s = f"{(_gp / _gl):.2f}" if _gl > 0 else "∞"
    _exp  = (sum(_pnls) / total_pts) if total_pts else 0.0
    _pct  = lambda n: (n / total_pts * 100) if total_pts else 0.0
    _oth_s = f" | ❔ Lain {_n_oth}" if _n_oth else ""
    _breakdown = (
        f"🎯 TP2 {_n_tp2} ({_pct(_n_tp2):.0f}%) | 🟡 BE {_n_be} ({_pct(_n_be):.0f}%) | "
        f"🛑 SL {_n_sl} ({_pct(_n_sl):.0f}%){_oth_s}\n"
        f"⚖️ Expectancy: <b>{_exp:+.2f} USDT/trade</b> | Profit Factor: <b>{_pf_s}</b>\n"
    )

    return (
        f"📊 <b>{label}</b>\n"
        f"[{_fmt_wr_bar(wr_pts)}] WR (poin): <b>{wr_pts:.1f}%</b>  ({win_pts}.0/{total_pts}.0 poin)\n"
        f"✅ {n_win} WIN ❌ {n_loss} LOSS | Total: {total_pts}\n"
        f"📌 Sistem poin: TP2=1pt | TP1=1pt | BE/SL=0pt\n"
        f"{_breakdown}"
        f"{net_em} Net PnL: <b>{sgn}{net_pct:.2f}%</b>\n"
        f"📈 Total WIN: <b>+{total_win_pct:.2f}%</b> | 📉 Total LOSS: <b>{total_loss_pct:.2f}%</b>\n"
        f"{net_em} Net keseluruhan: <b>{sgn}{net_pct:.2f}%</b>\n"
        f"📈 Avg Win: <b>+{avg_win_pct:.2f}%</b> | 📉 Avg Loss: <b>{avg_loss_pct:.2f}%</b>\n"
    )


def cmd_recap(parts: list = None):
    """
    /recap  atau  /recap 7 / /recap 30
    Kirim recap performa manual — TIDAK terkirim otomatis, hanya saat
    user tap tombol atau ketik command ini sendiri.

    Format:
      📊 SEMUA WAKTU
      WR (poin): xx.x% (win.0/total.0 poin)
      ✅ WIN ❌ LOSS | Total
      📌 Sistem poin: TP2=1pt | TP1=1pt | SL=0pt
      Net PnL, Total WIN, Total LOSS (dalam %, berbasis pnl_usdt)
      Avg Win / Avg Loss
      🕐 10 TRADE TERAKHIR (list simbol, arah, close_type, pnl%, tanggal)
    """
    days = None
    label = "SEMUA WAKTU"
    if parts and len(parts) > 1:
        try:
            days = int(parts[1])
            label = f"{days} HARI TERAKHIR"
        except ValueError:
            send_telegram_raw("⚠️ Format salah. Contoh: <code>/recap</code> atau <code>/recap 30</code>")
            return

    log = _load_trade_log()
    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        rows = []
        for t in log:
            try:
                ts = datetime.fromisoformat(t["ts"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    rows.append(t)
            except Exception:
                pass
    else:
        rows = log

    if not rows:
        send_telegram_raw("📭 Belum ada trade yang tercatat di log — belum ada yang bisa direcap.")
        return

    if days is None:
        _now = datetime.now(timezone.utc)
        _month_rows = []
        for t in rows:
            try:
                _ts = datetime.fromisoformat(t["ts"])
                if _ts.tzinfo is None:
                    _ts = _ts.replace(tzinfo=timezone.utc)
                if _ts.year == _now.year and _ts.month == _now.month:
                    _month_rows.append(t)
            except Exception:
                pass
        header = (
            _recap_block(_month_rows, f"BULAN INI ({_now.strftime('%Y-%m')})")
            + f"{'─'*34}\n"
            + _recap_block(rows, label)
        )
    else:
        header = _recap_block(rows, label)

    # ── 10 trade terakhir ──────────────────────────────────────────────────
    last10 = rows[-10:][::-1]  # terbaru dulu
    trade_lines = []
    for t in last10:
        symbol   = str(t.get("symbol", "?"))
        if symbol.endswith("USDT"):
            symbol = symbol[:-4]   # SUIUSDT -> SUI (biar tampil SUI/USDT, bukan SUIUSDT/USDT)
        direction = t.get("direction", "?")
        ct       = t.get("close_type", "?")
        pnl_pct  = _recap_pnl_pct(t)
        is_win   = _recap_is_win(t)
        mark     = "✅" if is_win else "❌"
        dir_dot  = "🟢" if direction == "LONG" else "🔴"
        try:
            ts = datetime.fromisoformat(t["ts"])
            date_str = ts.strftime("%Y-%m-%d")
        except Exception:
            date_str = "?"
        pnl_note = " (PnL negatif)" if (ct == "TP2" and pnl_pct < 0) else ""
        sign = "+" if pnl_pct >= 0 else ""
        trade_lines.append(
            f"{mark} {dir_dot} {symbol}/USDT {direction} | {ct}{pnl_note} | "
            f"{sign}{pnl_pct:.2f}% | {date_str}"
        )

    body = (
        f"{'─'*34}\n"
        f"🕐 <b>10 TRADE TERAKHIR:</b>\n" + "\n".join(trade_lines) + "\n"
        f"{'─'*34}\n"
        f"⚠️ PnL tanpa leverage. TP1/TP2 dihitung poin, bukan realized %.\n"
        f"💡 Tap tombol 📊 Recap kapan saja — recap ini TIDAK dikirim otomatis."
    )

    send_telegram_raw(header + body)









def tg_get_updates(offset: int) -> list:
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": offset, "timeout": 20},   # ← FIX: long-poll 20s, cegah fetch ganda
            timeout=25,                                   # ← FIX: harus lebih besar dari long-poll timeout
        )
        data = r.json()
        return data.get("result", [])
    except Exception:
        return []


def cmd_pnl():
    """
    Kirim unrealized PnL semua posisi aktif.
    Data diambil LANGSUNG dari Binance /fapi/v2/positionRisk — bukan dari
    active_positions saja — sehingga posisi manual atau posisi yang tidak
    ter-sync juga ikut tampil.
    Keterangan ikon:
      🤖 = dibuka/dikelola bot
      📱 = dibuka manual di aplikasi (tidak ada di active_positions)
    """
    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")

    # Ambil semua posisi langsung dari Binance
    try:
        all_risk = api_get("/fapi/v2/positionRisk", signed=True)
        real_positions = [p for p in all_risk if float(p.get("positionAmt", 0)) != 0]
    except Exception as e:
        send_telegram_raw(
            f"💹 <b>/pnl — {now_str}</b>\n"
            f"{'─'*34}\n"
            f"❌ Gagal ambil data dari Binance: <code>{e}</code>"
        )
        return

    if not real_positions:
        send_telegram_raw(
            f"💹 <b>/pnl — {now_str}</b>\n"
            f"{'─'*34}\n"
            f"📭 Tidak ada posisi aktif di Binance."
        )
        return

    total_pnl = 0.0
    lines     = []

    for p in real_positions:
        symbol      = p["symbol"]
        amt         = float(p["positionAmt"])
        entry_price = float(p["entryPrice"])
        mark_price  = float(p["markPrice"])
        unrealized  = float(p["unRealizedProfit"])
        direction   = "LONG" if amt > 0 else "SHORT"
        dir_em      = "🟢" if direction == "LONG" else "🔴"
        src_em      = "🤖" if symbol in active_positions else "📱"

        total_pnl += unrealized
        pnl_em    = "🟢" if unrealized >= 0 else "🔴"
        pnl_str   = f"{pnl_em} {'+'if unrealized>=0 else ''}{unrealized:.2f} USDT"

        # Ambil entry_type dari active_positions jika ada
        _pos_data   = active_positions.get(symbol, {})
        _entry_type = _pos_data.get("entry_type", "")
        _type_line  = f"\n  Type : {_entry_type}" if _entry_type else ""

        lines.append(
            f"{dir_em}{src_em} <b>{symbol}</b> {direction}\n"
            f"  Entry: {entry_price:.4f} | Mark: {mark_price:.4f}{_type_line}\n"
            f"  PnL  : {pnl_str}"
        )

    total_em  = "🟢" if total_pnl >= 0 else "🔴"
    total_str = f"{total_em} {'+'if total_pnl>=0 else ''}{total_pnl:.2f} USDT"
    bot_count    = sum(1 for p in real_positions if p["symbol"] in active_positions)
    manual_count = len(real_positions) - bot_count

    # ── Cumulative PNL harian & bulanan ─────────────────────────────────────
    try:
        current_bal = get_total_balance()
        # FIX: handle None (API error)
        if current_bal is not None:
            refresh_pnl_snapshots(current_bal)
            pnl_summary = get_pnl_summary_lines(current_bal)
            bal_line    = f"💼 Balance : <b>{current_bal:.2f} USDT</b>\n"
        else:
            pnl_summary = ""
            bal_line    = "💼 Balance : <b>N/A (API error)</b>\n"
    except Exception:
        pnl_summary = ""
        bal_line    = ""

    # ── Pending limit orders (belum terisi) ──────────────────────────────────
    pending_block = ""
    if pending_limit_orders:
        pend_lines = []
        for _psym, _pdata in pending_limit_orders.items():
            _pdir    = _pdata.get("direction", "?")
            _pem     = "🟢" if _pdir == "LONG" else "🔴"
            _pentry  = _pdata.get("entry", 0)
            _ptype   = _pdata.get("entry_type", "📊 Bot")
            _placed  = _pdata.get("placed_at")
            _age_str = ""
            if _placed:
                try:
                    _age_min = int((datetime.now(timezone.utc) - _placed).total_seconds() / 60)
                    _age_str = f" | {_age_min}m lalu"
                except Exception:
                    pass
            pend_lines.append(
                f"{_pem}⏳ <b>{_psym}</b> {_pdir}\n"
                f"  Entry: {_pentry:.4f} | {_ptype}{_age_str}"
            )
        pending_block = (
            f"\n{'─'*34}\n"
            f"⏳ <b>Pending Limit Orders ({len(pending_limit_orders)})</b>\n"
            + "\n".join(pend_lines)
        )

    mode_pnl_block = get_mode_pnl_block()
    cumulative_block = (
        f"\n{'─'*34}\n"
        f"📈 <b>Cumulative PNL</b>\n"
        f"{pnl_summary}\n"
        f"{'─'*34}\n"
        f"📊 <b>PnL per Mode</b>\n"
        f"{mode_pnl_block}\n"
        f"💡 /resetpnl — reset PnL mode aktif"
    ) if (pnl_summary or True) else ""

    msg = (
        f"💹 <b>/pnl — {now_str}</b>\n"
        f"{'─'*34}\n"
        + ("\n" + "─"*34 + "\n").join(lines) +
        f"\n{'─'*34}\n"
        f"💹 Unrealized: <b>{total_str}</b>\n"
        f"{bal_line}"
        f"📊 Posisi: <b>{len(real_positions)}</b> (🤖{bot_count} | 📱{manual_count})"
        f"{pending_block}"
        f"{cumulative_block}"
    )
    send_telegram_raw(msg)


def cmd_status():
    """Kirim status lengkap bot."""
    now     = datetime.now(timezone.utc)
    now_str = now.strftime("%d %b %Y %H:%M UTC")
    total   = bot_state["wins"] + bot_state["losses"]
    winrate = (bot_state["wins"] / total * 100) if total > 0 else 0

    try:
        bal = get_total_balance()
        # FIX: handle None (API error)
        bal_str = f"{bal:.2f} USDT" if bal is not None else "N/A (API error)"
    except Exception:
        bal_str = "N/A"

    uptime_str = "N/A"
    if bot_state.get("start_time"):
        up_secs = int((now - bot_state["start_time"]).total_seconds())
        up_h = up_secs // 3600
        up_m = (up_secs % 3600) // 60
        uptime_str = f"{up_h}j {up_m}m"

    state_em  = "⏸ PAUSED" if bot_paused else "▶️ RUNNING"
    mode_em   = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"
    pre_count = sum(1 for p in active_positions.values() if p.get("pre_existing"))

    pos_lines = []
    for symbol, pos in active_positions.items():
        dir_em = "🟢" if pos["direction"] == "LONG" else "🔴"
        pre_em = "📥" if pos.get("pre_existing") else "🤖"
        price  = get_current_price(symbol)
        price_str = f"{price:,.4f}" if price else "N/A"
        pos_lines.append(f"  {dir_em}{pre_em} {symbol} {pos['direction']} | Entry:{pos['entry']} | Now:{price_str}")

    pos_block = "\n".join(pos_lines) if pos_lines else "  📭 Tidak ada posisi aktif"

    # ── Score threshold info ─────────────────────────────────────────────────
    if MIN_SCORE_CUSTOM > 0:
        score_line = (
            f"🧮 Score Gate : FULL ≥<b>{MIN_SCORE_CUSTOM}</b> | "
            f"RELAXED ≥<b>{MIN_SCORE_RELAXED_CUSTOM}</b> (custom)"
        )
    else:
        score_line = (
            f"🧮 Score Gate : FULL ≥<b>{MIN_SCORE}</b> | "
            f"RELAXED ≥<b>{MIN_SCORE_RELAXED}</b> (default)"
        )

    # ── BTC correlation filter info ──────────────────────────────────────────
    btc_corr_line = (
        f"🔗 BTC+Dom Filter: <b>{'✅ ON' if BTC_CORR_FILTER_ON else '⭕ OFF'}</b>"
    )

    # ── Scan mode filter info ────────────────────────────────────────────────
    active_mode_labels = [m["label"] for m in get_active_modes()]
    mode_filter_em = {"ALL": "🔄", "SCALPING": "⚡", "INTRADAY": "📈"}.get(_ACTIVE_MODE_FILTER, "🔄")
    scan_mode_line = (
        f"📡 Scan Mode  : {mode_filter_em} <b>{_ACTIVE_MODE_FILTER}</b> "
        f"({', '.join(active_mode_labels)})"
    )
    dir_filter_em = {"ALL": "🔄", "LONG": "🟢", "SHORT": "🔴"}.get(_DIRECTION_FILTER, "🔄")
    dir_filter_line = (
        f"🧭 Arah Filter: {dir_filter_em} <b>{_DIRECTION_FILTER}</b>"
    )
    # BTC situational info for /status
    _sit_icons = {
        "BTC_NEAR_DEMAND_BOUNCE":  "⚠️ BTC Near Demand — SHORT alt DIBLOK",
        "BTC_NEAR_DEMAND_FALLING": "⚡ BTC Near Demand — SHORT berisiko",
        "BTC_NEAR_SUPPLY_REJECT":  "⚠️ BTC Near Supply — LONG alt DIBLOK",
        "BTC_NEAR_SUPPLY_RISING":  "⚡ BTC Near Supply — LONG berisiko",
        "BTC_TRENDING_BULLISH":    "🟢 BTC Strong Up — bonus LONG",
        "BTC_TRENDING_BEARISH":    "🔴 BTC Strong Down — bonus SHORT",
        "BTC_EXTENDED":            "➡️ BTC Ranging — sinyal lebih selektif",
        "BTC_NEUTRAL":             "🔄 BTC Neutral",
    }
    try:
        _sit_df_ltf = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
        _sit_df_htf = fetch_ohlcv_safe("BTC/USDT", "1h", limit=100)
        _sit_btcd   = fetch_btcd_ohlcv_isolated(tf=BTCDOM_LTF_TF, limit=80)
        _sit_ltf_dir = get_btc_ltf_direction(_sit_df_ltf)
        _sit_btcd_dir = get_btcd_ltf_direction(_sit_btcd)
        _sit_result  = analyze_btc_situation(
            df_btc_ltf    = _sit_df_ltf,
            df_btc_htf    = _sit_df_htf,
            btc_bias_htf  = get_btc_bias(),
            btcd_ltf_dir  = _sit_btcd_dir,
            btcd_trend_htf= get_btcd_bias(),
        )
        _sit_label = _sit_icons.get(_sit_result["situation"], "🔄 BTC Neutral")
        btc_situation_line = f"🪙 BTC Situation: <b>{_sit_label}</b>"
    except Exception:
        btc_situation_line = "🪙 BTC Situation: <i>data tidak tersedia</i>"

    # FIX: sebelumnya selalu nampilin MAX_SL_DISTANCE_PCT (constant global lama,
    # dulu 3%) walau cap yang BENERAN dipakai untuk mode aktif berasal dari
    # SL_TP_CAPS per-mode (mis. HTF_1H = 7% sekarang) — notif jadi menyesatkan,
    # nunjukin angka yang beda dari cap yang sebenarnya berlaku. Sekarang ambil
    # dari SL_TP_CAPS sesuai mode aktif (_ALL_MODES[0]), fallback ke
    # MAX_SL_DISTANCE_PCT kalau entah kenapa mode tidak ketemu di dict.
    _status_active_label = _ALL_MODES[0]["label"] if _ALL_MODES else None
    _status_sl_cap, _ = SL_TP_CAPS.get(_status_active_label, (MAX_SL_DISTANCE_PCT, 0))
    sl_filter_line = (
        f"🛑 SL Filter  : <b>max {_status_sl_cap*100:.1f}%</b> dari entry "
        f"({'aktif' if _status_sl_cap > 0 else 'OFF'})"
    )

    # ── Super Scalper Mode badge ─────────────────────────────────────────────
    super_scalper_line = ""
    if _SUPER_SCALPER_MODE:
        super_scalper_line = (
            f"\n⚡⚡ <b>SUPER SCALPER MODE: ON</b> ⚡⚡\n"
            f"  Pairs: {len(_SUPER_SCALPER_PAIRS)} | Score≥45 | RR≥1.8 | SL≤1.2% | TP1=0.6% | SL CD=1j"
        )

    msg = (
        f"🤖 <b>BOT STATUS — {now_str}</b>\n"
        f"{'─'*38}\n"
        f"{_bot_version_line_html()}"
        f"{'─'*38}\n"
        f"⚡ State      : <b>{state_em}</b>\n"
        f"🌐 Mode       : <b>{mode_em}</b>\n"
        f"⏱ Uptime     : {uptime_str}\n"
        f"💼 Balance    : <b>{bal_str}</b>\n"
        f"📡 Data Src   : {_active_data_source}\n"
        f"🚀 Scan Config: interval={SCAN_INTERVAL}s, workers={MAX_WORKERS}\n"
        f"{'─'*38}\n"
        f"📊 Performance\n"
        f"  ✅ Win      : {bot_state['wins']}\n"
        f"  ❌ Loss     : {bot_state['losses']}\n"
        f"  🎯 Winrate  : {winrate:.1f}%\n"
        f"  🔔 Sinyal   : {bot_state['signals_today']} hari ini\n"
        f"  🧮 Trade    : {get_trades_today()}"
        f"{('/' + str(MAX_TRADES_PER_DAY)) if MAX_TRADES_PER_DAY > 0 else ' (tanpa batas)'} terisi hari ini\n"
        f"{'─'*38}\n"
        f"📌 Posisi Aktif ({len(active_positions)}, {pre_count} pre-existing):\n"
        f"{pos_block}\n"
        f"{'─'*38}\n"
        f"⚙️ Config:\n"
        f"  Max SL {MAX_SL_LOSS_PCT*100:.1f}%/trade | Max {MAX_OPEN_TRADES} trades | Lev auto-tier\n"
        f"  {score_line}\n"
        f"  {btc_corr_line}\n"
        f"  {scan_mode_line}\n"
        f"  {dir_filter_line}\n"
        f"  {btc_situation_line}\n"
        f"  {sl_filter_line}"
        f"{super_scalper_line}"
    )
    send_telegram_raw(msg)




def cmd_resumeorpause():
    """Toggle pause/resume bot scanning & trading."""
    global bot_paused
    bot_paused = not bot_paused

    if bot_paused:
        msg = (
            f"⏸ <b>Bot di-PAUSE</b>\n"
            f"{'─'*30}\n"
            f"Scanning & trading dihentikan sementara.\n"
            f"Posisi aktif tetap dimonitor.\n"
            f"Ketik /resumeorpause untuk melanjutkan."
        )
    else:
        _daily_limit_state["paused_by"] = None  # reset agar limit/drawdown bisa re-trigger
        msg = (
            f"▶️ <b>Bot di-RESUME</b>\n"
            f"{'─'*30}\n"
            f"Scanning & trading aktif kembali.\n"
            f"Ketik /resumeorpause untuk pause."
        )
    send_telegram_raw(msg)
    print(f"{'⏸ PAUSED' if bot_paused else '▶️ RESUMED'}")
    save_state()


def cmd_start():
    """
    /start — Resume bot dari kondisi PAUSED saat startup.
    Bisa dipakai kapan saja untuk pastikan bot dalam kondisi RUNNING.
    """
    global bot_paused
    if not bot_paused:
        send_telegram_raw(
            f"▶️ <b>Bot sudah berjalan!</b>\n"
            f"Gunakan /resumeorpause untuk pause, atau /status untuk cek kondisi."
        )
        return

    # ── Pre-flight check: pastikan API key valid & IP sudah di-whitelist ──────
    # Cek ini SEBELUM bot dianggap aktif, supaya kalau IP salah/API key invalid,
    # bot TETAP paused (bukan lanjut jalan lalu baru gagal pas order pertama).
    mode_em_check = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"
    send_telegram_raw(f"🔎 <b>Cek koneksi ke Binance ({mode_em_check})...</b>")
    ok, check_msg = verify_api_connectivity()
    if not ok:
        send_telegram_raw(
            f"⛔ <b>/start DITOLAK — Koneksi Binance Gagal</b>\n"
            f"{'─'*34}\n"
            f"🌐 Mode: <b>{mode_em_check}</b>\n"
            f"❌ {check_msg}\n"
            f"{'─'*34}\n"
            f"Bot TETAP standby (belum trading). Perbaiki masalah di atas, "
            f"lalu kirim /start lagi."
        )
        print(f"⛔ /start DITOLAK — verify_api_connectivity gagal: {check_msg}")
        return

    bot_paused = False
    _daily_limit_state["paused_by"] = None  # reset agar drawdown/limit bisa re-trigger jika perlu

    # ── FIX (2026-09-15): reload exchange info sesuai mode FINAL ──────────────
    # load_exchange_info() di startup main() jalan SEBELUM user konfirmasi mode
    # lewat /start (base URL saat itu bisa saja belum final kalau load_state()
    # mengubah BOT_MODE setelahnya). Reload di sini supaya _exchange_info_cache
    # — yang dipakai get_active_pairs() buat filter pair testnet-vs-mainnet —
    # selalu sesuai base URL yang BENAR-BENAR dipakai bot mulai sekarang.
    try:
        load_exchange_info()
        _skipped_pairs = [p for p in PAIR_LIST if p not in _exchange_info_cache]
        if _skipped_pairs:
            _mode_label = "DEMO (testnet)" if BOT_MODE == "DEMO" else "LIVE (mainnet)"
            send_telegram_raw(
                f"ℹ️ <b>{len(_skipped_pairs)} pair di-skip — tidak listed di {_mode_label}</b>\n"
                f"{'─'*34}\n"
                f"{', '.join(sorted(_skipped_pairs))}\n"
                f"Pair ini otomatis dilewati dari scan (bukan error) supaya "
                f"tidak muncul 'Invalid symbol' (-1121) berulang saat entry."
            )
    except Exception as _ei_err:
        print(f"⚠️ Reload exchange info di /start gagal: {_ei_err}")

    # ── Sync posisi BARU setelah user konfirmasi via /start ───────────────────
    # Ini sengaja ditunda dari startup agar tidak false-close posisi LIVE
    # karena bot sempat konek ke endpoint yang salah saat masih paused.
    # Sekarang user sudah pastikan mode benar → aman untuk sync.
    send_telegram_raw("🔄 <b>Menghubungkan ke Binance...</b> sync posisi aktif...")
    try:
        # FIX KRITIS: sebelumnya active_positions.clear() di sini membuang SEMUA
        # progress posisi yang sudah di-restore load_state() saat boot — termasuk
        # tp1_hit, original_sl, trailing_active, dan unprotected. Akibatnya posisi
        # yang TP1-nya sudah kena (SL seharusnya sudah di breakeven) diproses ulang
        # oleh sync_existing_positions() sebagai posisi "baru" dari nol (tp1_hit
        # dipaksa False lagi), dan kalau SL di Binance saat itu kebetulan kosong
        # (mis. sedang dalam proses ganti SL saat restart terjadi), posisi bisa
        # berakhir tanpa proteksi yang benar tanpa ada peringatan berkelanjutan.
        #
        # Fix: simpan dulu snapshot posisi lama sebelum clear, lalu setelah
        # sync_existing_positions() jalan (yang tetap perlu clear dulu supaya bisa
        # menangani ganti mode LIVE/DEMO dengan benar), gabungkan kembali field-field
        # progress kritis untuk symbol yang sama-sama muncul di kedua sisi — supaya
        # histori tp1_hit/breakeven tidak pernah hilang gara-gara restart/`/start`.
        _pre_start_positions = {k: dict(v) for k, v in active_positions.items()}

        active_positions.clear()          # bersihkan snapshot lama (mungkin dari mode lain)
        sync_existing_positions()         # ambil posisi real dari Binance sesuai BOT_MODE saat ini

        _CRITICAL_PROGRESS_FIELDS = (
            "tp1_hit", "tp1_hit_at", "original_sl", "trailing_active",
            "unprotected", "sl", "tp2_order_ref", "open_time", "mode",
            "entry_tf", "score", "pre_existing",
        )
        for _sym, _old in _pre_start_positions.items():
            if _sym not in active_positions:
                continue  # posisi lama sudah tidak ada di Binance — memang harus hilang
            for _f in _CRITICAL_PROGRESS_FIELDS:
                if _f in _old and _old[_f] not in (None, False, 0):
                    active_positions[_sym][_f] = _old[_f]
            # Kalau posisi lama sudah tp1_hit tapi SL hasil sync sekarang kosong/0
            # (artinya SL memang belum berhasil terpasang ulang sebelum restart) —
            # tandai unprotected EKSPLISIT supaya cabang retry di manage_trailing()
            # langsung ambil alih di scan berikutnya, bukan diam menganggap aman.
            if _old.get("tp1_hit") and not active_positions[_sym].get("sl"):
                active_positions[_sym]["unprotected"] = True
                print(f"  🚨 [{_sym}] Restore /start: tp1_hit=True tapi SL kosong hasil sync — ditandai unprotected untuk retry segera")

        if _pre_start_positions:
            save_state()  # persist hasil merge supaya konsisten di disk juga

        pos_count = len(active_positions)
        pre_count = sum(1 for p in active_positions.values() if p.get("pre_existing"))
        if pos_count > 0:
            pos_note = f"\n📌 Posisi terdeteksi: <b>{pos_count}</b> ({pre_count} pre-existing, tetap dimonitor)"
        else:
            pos_note = "\n📭 Tidak ada posisi aktif saat ini."
    except Exception as e:
        pos_note = f"\n⚠️ Sync posisi gagal: {e}"
        print(f"⚠️ sync_existing_positions gagal saat /start: {e}")

    # ── Auto cleanup orphan conditional orders (tanpa bilang ke user — cukup log) ──
    # Setelah sync posisi, langsung bersihkan conditional orders yang tidak punya
    # posisi aktif. Ini menangani kasus: posisi close tapi SL/TP orphan tertinggal.
    # Dijalankan otomatis — user tidak perlu ketik /cleanuporders manual.
    try:
        print("  🧹 Auto-cleanup orphan orders saat startup...")
        cleanup_stale_orders(dry_run=False, silent_if_clean=True)
    except Exception as _cleanup_err:
        print(f"  ⚠️  Auto-cleanup startup gagal (non-fatal): {_cleanup_err}")

    try:
        bal = get_total_balance()
        # FIX: handle None (API error)
        bal_str = f"{bal:.2f} USDT" if bal is not None else "N/A"
    except Exception:
        bal_str = "N/A"

    mode_em = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"
    send_telegram_raw(
        f"🚀 <b>Bot STARTED!</b>\n"
        f"{'─'*34}\n"
        f"▶️ Scanning & trading aktif.\n"
        f"💼 Balance   : <b>{bal_str}</b>\n"
        f"🌐 Mode      : <b>{mode_em}</b>\n"
        f"⚡ Max SL/trade: <b>{MAX_SL_LOSS_PCT*100:.1f}%</b> dari balance | Max: <b>{MAX_OPEN_TRADES}</b> trades\n"
        f"{'─'*34}\n"
        f"Gunakan /status untuk cek kondisi bot."
        f"{pos_note}"
    )
    print("▶️ Bot STARTED via /start command")
    save_state()


def _close_positions_filtered(pnl_filter, label: str, cmd_tag: str):
    """
    Tutup posisi aktif di Binance yang lolos pnl_filter(unrealized_pnl) -> bool.
    Sumber: query langsung dari Binance positionRisk — BUKAN hanya active_positions
    memory. Ini memastikan posisi pre-existing / dibuka manual juga ikut tertutup.

    pnl_filter menerima unrealized PnL (float, dari field unRealizedProfit Binance)
    dan return True kalau posisi itu harus ditutup.
    label   = teks untuk pesan Telegram (mis. "SEMUA", "LOSS", "PROFIT")
    cmd_tag = nama dipakai di set_cooldown_post_close reason
    """
    send_telegram_raw("🔄 <b>Mengecek posisi di Binance...</b>")

    # ── Ambil semua posisi real dari Binance (bukan dari memory bot) ──────────
    try:
        all_risk = api_get("/fapi/v2/positionRisk", signed=True)
    except Exception as e:
        send_telegram_raw(f"❌ Gagal ambil posisi dari Binance: {e}")
        return

    # Filter hanya yang benar-benar open (positionAmt != 0)
    open_positions = [p for p in all_risk if float(p.get("positionAmt", 0)) != 0]

    if not open_positions:
        send_telegram_raw("📭 Tidak ada posisi aktif untuk ditutup.")
        return

    # ── Filter berdasarkan unrealized PnL langsung dari Binance ──────────────
    target_positions = []
    for p in open_positions:
        try:
            _u_pnl = float(p.get("unRealizedProfit", 0))
        except Exception:
            _u_pnl = 0.0
        if pnl_filter(_u_pnl):
            target_positions.append(p)

    if not target_positions:
        send_telegram_raw(f"📭 Tidak ada posisi <b>{label}</b> untuk ditutup ({len(open_positions)} posisi aktif total).")
        return

    send_telegram_raw(
        f"🔄 <b>Menutup posisi {label}...</b>\n"
        f"Total: {len(target_positions)} dari {len(open_positions)} posisi aktif"
    )

    closed = []
    failed = []

    for p in target_positions:
        symbol   = p["symbol"]
        pos_amt  = float(p["positionAmt"])
        # positionAmt positif = LONG, negatif = SHORT
        direction  = "LONG" if pos_amt > 0 else "SHORT"
        close_side = "SELL" if direction == "LONG" else "BUY"
        abs_qty    = abs(pos_amt)

        try:
            # Cancel semua open orders untuk symbol ini dulu
            cancel_open_orders(symbol)
            time.sleep(0.2)

            # Pakai mkt_step_size dan quantityPrecision agar qty tidak ditolak Binance
            # FIX: langsung pakai format_qty yang sudah handle floor + presisi ketat.
            # Menghindari double-rounding yang bisa meninggalkan digit ekstra
            # (misal TAOUSDT positionAmt=35.411 tapi quantityPrecision=2 → harus "35.41")
            filters       = get_lot_filters(symbol)
            mkt_step_size = filters.get("mkt_stepSize", filters.get("stepSize", 0.001))
            cl_qty_prec   = filters.get("quantityPrecision", None)
            # FIX -4005 / FIX proteksi-parsial: qty > mkt_maxQty TIDAK BOLEH
            # di-clamp — clamp lama hanya menutup sebagian posisi lalu
            # SISANYA TETAP TERBUKA tanpa disadari (bug sama seperti di
            # algo_post_sl/algo_post_tp). Sekarang: pecah jadi beberapa
            # MARKET order berturut-turut sampai qty benar-benar habis.
            mkt_max_qty   = filters.get("mkt_maxQty", filters.get("maxQty", 9999999.0))
            pos_side_field = p.get("positionSide", "BOTH")

            def _send_close_chunk(_qty: float) -> dict:
                _qty_str = format_qty(_qty, mkt_step_size, cl_qty_prec)
                _params = {
                    "symbol":   symbol,
                    "side":     close_side,
                    "type":     "MARKET",
                    "quantity": _qty_str,
                }
                if pos_side_field in ("LONG", "SHORT"):
                    _params["positionSide"] = pos_side_field
                else:
                    _params["reduceOnly"] = "true"
                print(f"  📤 CLOSE PARAMS [{symbol}]: {_params}")
                _sync_server_time(force=True)  # FIX v39e: force sync sebelum close order
                _resp = api_post("/fapi/v1/order", _params)
                print(f"  ✅ CLOSE RESP [{symbol}]: orderId={_resp.get('orderId')} status={_resp.get('status')} executedQty={_resp.get('executedQty')}")
                send_telegram_raw(
                    f"📤 <b>Order close terkirim — {symbol}</b>\n"
                    f"orderId: <code>{_resp.get('orderId')}</code>\n"
                    f"status: <code>{_resp.get('status')}</code>\n"
                    f"executedQty: <code>{_resp.get('executedQty')}</code>\n"
                    f"side: <code>{_params.get('side')}</code> | qty: <code>{_params.get('quantity')}</code>\n"
                    f"positionSide: <code>{_params.get('positionSide', 'BOTH')}</code> | reduceOnly: <code>{_params.get('reduceOnly', 'false')}</code>"
                )
                return _resp, _qty_str

            if mkt_max_qty and abs_qty > mkt_max_qty:
                print(f"  🔻 [{symbol}] closeall qty={abs_qty} > mkt_maxQty={mkt_max_qty} → split jadi beberapa order (BUKAN di-clamp)")
                _remaining = abs_qty
                resp = None
                close_qty_str = "0"
                while _remaining > 0:
                    _chunk = min(_remaining, mkt_max_qty)
                    resp, close_qty_str = _send_close_chunk(_chunk)
                    _remaining = round(_remaining - _chunk, 10)
                    if _remaining > 0:
                        time.sleep(0.2)
            else:
                resp, close_qty_str = _send_close_chunk(abs_qty)

            # Estimasi PnL
            entry_price = float(p.get("entryPrice", 0))
            cur_price   = get_current_price(symbol)
            pnl_str     = "N/A"
            is_win      = False
            if cur_price and entry_price:
                pnl    = (cur_price - entry_price) * abs_qty if direction == "LONG" else (entry_price - cur_price) * abs_qty
                pnl_em = "🟢" if pnl >= 0 else "🔴"
                pnl_str = f"{pnl_em} {'+'if pnl>=0 else ''}{pnl:.2f} USDT"
                is_win  = pnl >= 0

            _perf_pnl = pnl if (cur_price and entry_price) else (1 if is_win else -1)
            update_performance(_perf_pnl)
            closed.append((symbol, direction, pnl_str))

            # Hapus dari memory bot jika ada
            if symbol in active_positions:
                with _positions_lock:   # FIX BUG #3: thread-safe delete
                    active_positions.pop(symbol, None)
            set_cooldown_post_close(symbol, reason=f"manual {cmd_tag}")

            print(f"🔴 CLOSED {symbol} {direction} qty={close_qty_str} | PnL: {pnl_str}")

        except Exception as e:
            failed.append((symbol, str(e)))
            print(f"❌ Gagal close {symbol}: {e}")

    # ── Kirim laporan hasil ───────────────────────────────────────────────────
    lines = []
    for sym, dir_, pnl_s in closed:
        lines.append(f"  ✅ {sym} {dir_} | PnL: {pnl_s}")
    for sym, err in failed:
        lines.append(f"  ❌ {sym} GAGAL: {err}")

    wins  = bot_state["wins"]
    losses = bot_state["losses"]
    total = wins + losses
    wr    = (wins / total * 100) if total > 0 else 0
    msg = (
        f"🔴 <b>Close Posisi {label} — Selesai</b>\n"
        f"{'─'*34}\n"
        + "\n".join(lines) +
        f"\n{'─'*34}\n"
        f"✅ {len(closed)} ditutup | ❌ {len(failed)} gagal\n"
        f"{'─'*34}\n"
        f"📊 Total: ✅ Win: <b>{wins}</b> | ❌ Loss: <b>{losses}</b> | 🎯 WR: <b>{wr:.1f}%</b>"
    )
    save_state()   # FIX: sinkron active_positions ke disk setelah close
    send_telegram_raw(msg)


def cmd_closeallposition():
    """Tutup SEMUA posisi aktif (tanpa filter PnL)."""
    _close_positions_filtered(lambda pnl: True, "SEMUA", "/closeallposition")


def cmd_closealllosing():
    """Tutup HANYA posisi yang sedang unrealized LOSS (pnl < 0)."""
    _close_positions_filtered(lambda pnl: pnl < 0, "LOSS 🔴", "/closealllosing")


def cmd_closeallprofit():
    """Tutup HANYA posisi yang sedang unrealized PROFIT (pnl > 0)."""
    _close_positions_filtered(lambda pnl: pnl > 0, "PROFIT 🟢", "/closeallprofit")







def cmd_setmarginratio(parts: list):
    """
    /setmarginratio <persen>
    Set MAX_SL_LOSS_PCT — batas kerugian MAKSIMAL per trade jika SL kena,
    dihitung sebagai % dari BALANCE EFEKTIF (available balance dikurangi
    margin yang sudah terpakai posisi/pending order lain — persis basis
    yang dipakai execute_trade() saat sizing, BUKAN total balance mentah).

    Lot dihitung MUNDUR dari batas ini:
      lot = (effective_available × MAX_SL_LOSS_PCT) / eff_sl_dist
    → SL dekat: lot lebih besar. SL jauh: lot lebih kecil.
    → Kerugian aktual saat SL kena TIDAK AKAN MELEBIHI angka ini.

    Contoh: /setmarginratio 1   → jika SL kena, max rugi 1% dari balance efektif
            /setmarginratio 2   → jika SL kena, max rugi 2% dari balance efektif
    Range  : 0.1% – 20%
    """
    global MAX_SL_LOSS_PCT, MARGIN_RATIO

    # FIX KONSISTENSI: sebelumnya estimasi di sini pakai get_total_balance()
    # (total balance PENUH, tidak dikurangi apa pun), sedangkan execute_trade()
    # sizing beneran pakai _effective_available (= get_balance() dikurangi
    # margin pending limit orders symbol LAIN yang belum fill). Kalau ada
    # posisi/pending order lain jalan bersamaan, dua angka ini beda — user
    # lihat "max loss 47.64 USDT" di sini tapi realisasi SL kena cuma ~40
    # USDT karena _effective_available saat itu lebih kecil dari total
    # balance. Sekarang: estimasi di sini pakai basis yang SAMA PERSIS
    # dengan execute_trade() supaya angka yang ditampilkan match realisasi.
    def _estimate_effective_available():
        """Replikasi persis logika _effective_available di execute_trade()."""
        _avail = get_balance()
        if not _avail or _avail <= 0:
            return None, None
        _pending_margin = 0.0
        for _pend_sym, _pend in pending_limit_orders.items():
            try:
                _p_notional = (_pend.get("lot", 0) or 0) * (_pend.get("entry", 0) or 0)
                _p_lev      = PER_SYMBOL_LEVERAGE.get(_pend_sym, FIXED_LEVERAGE if FIXED_LEVERAGE > 0 else 5)
                _pending_margin += _p_notional / max(_p_lev, 1)
            except Exception:
                pass
        return max(0.0, _avail - _pending_margin), _avail

    if len(parts) < 2:
        try:
            eff_avail, raw_avail = _estimate_effective_available()
            bal = get_total_balance()
            if eff_avail is not None:
                max_loss_usdt = eff_avail * MAX_SL_LOSS_PCT
                _open_margin_note = ""
                if bal is not None and bal > eff_avail * 1.001:
                    _open_margin_note = (
                        f"\n📌 Ada margin terpakai (posisi/pending order lain): "
                        f"total balance {bal:.2f} → efektif {eff_avail:.2f} USDT"
                    )
                bal_line = (
                    f"💰 Max loss/trade saat ini: <b>{max_loss_usdt:.2f} USDT</b> "
                    f"(dari balance efektif {eff_avail:.2f}){_open_margin_note}"
                )
            elif bal is not None:
                # Fallback kalau get_balance() gagal tapi get_total_balance() sukses
                max_loss_usdt = bal * MAX_SL_LOSS_PCT
                bal_line = (
                    f"💰 Max loss/trade saat ini: <b>{max_loss_usdt:.2f} USDT</b> "
                    f"(dari total balance {bal:.2f} — balance efektif gagal diambil)"
                )
            else:
                bal_line = ""
        except Exception:
            bal_line = ""
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/setmarginratio 1</code>\n"
            "Contoh nilai: 0.5, 1, 2 (dalam persen max SL loss per trade)\n"
            f"Max SL loss saat ini: <b>{MAX_SL_LOSS_PCT * 100:.2f}%</b> dari balance efektif\n"
            f"{bal_line}\n"
            "ℹ️ Lot dihitung otomatis agar loss MAKSIMAL saat SL kena = angka ini.\n"
            "ℹ️ SL dekat → lot lebih besar | SL jauh → lot lebih kecil.\n"
            "ℹ️ Kalau SL sinyal lebih rapat dari floor minimum (0.8%), floor yang "
            "dipakai — jadi realisasi loss bisa sedikit di bawah angka di atas."
        )
        return

    try:
        new_pct = float(parts[1].replace(",", "."))
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka yang valid. Contoh: <code>/setmarginratio 1</code>")
        return

    if not (0.1 <= new_pct <= 20.0):
        send_telegram_raw(
            "⚠️ Max SL loss harus antara <b>0.1%</b> sampai <b>20%</b>.\n"
            "Disarankan: 0.5–3% untuk manajemen risiko yang sehat."
        )
        return

    old_pct          = MAX_SL_LOSS_PCT * 100
    MAX_SL_LOSS_PCT  = new_pct / 100.0
    MARGIN_RATIO     = MAX_SL_LOSS_PCT   # alias

    if new_pct <= 1.0:
        risk_note = "🟢 Konservatif — risiko kecil per trade"
    elif new_pct <= 3.0:
        risk_note = "🟡 Moderat — standar risk management"
    elif new_pct <= 7.0:
        risk_note = "🟠 Agresif — hati-hati drawdown"
    else:
        risk_note = "🔴 Sangat agresif — risiko tinggi!"

    try:
        eff_avail, raw_avail = _estimate_effective_available()
        bal = get_total_balance()
        if eff_avail is not None:
            max_loss_usdt = eff_avail * MAX_SL_LOSS_PCT
            bal_line      = (
                f"💰 Max loss/trade: <b>{max_loss_usdt:.2f} USDT</b> "
                f"(dari balance efektif {eff_avail:.2f})"
            )
        elif bal is not None:
            max_loss_usdt = bal * MAX_SL_LOSS_PCT
            bal_line      = (
                f"💰 Max loss/trade: <b>{max_loss_usdt:.2f} USDT</b> "
                f"(dari total balance {bal:.2f} — balance efektif gagal diambil)"
            )
        else:
            bal_line = ""
    except Exception:
        bal_line = ""

    send_telegram_raw(
        f"✅ <b>Max SL Loss Per Trade Diubah</b>\n"
        f"{'─'*34}\n"
        f"📉 Sebelum : <b>{old_pct:.2f}%</b>\n"
        f"📈 Sekarang: <b>{new_pct:.2f}%</b> dari total balance\n"
        f"{'─'*34}\n"
        f"{bal_line}\n"
        f"{risk_note}\n"
        f"{'─'*34}\n"
        f"ℹ️ Lot dihitung mundur dari batas ini:\n"
        f"   lot = (balance × {new_pct:.1f}%) ÷ jarak_SL\n"
        f"ℹ️ Loss aktual saat SL kena ≤ {new_pct:.2f}% balance.\n"
        f"ℹ️ Berlaku untuk trade baru. Posisi aktif tidak berubah."
    )
    print(f"⚙️ MAX_SL_LOSS_PCT diubah: {old_pct:.2f}% → {new_pct:.2f}%")
    save_state()


def cmd_setscoreupto(parts: list):
    """
    /setscoreupto <skor>            → FULL & RELAXED sama-sama pakai <skor>
    /setscoreupto <full> <relaxed>  → set terpisah per tier
    /setscoreupto 0                 → balik ke default (MIN_SCORE / MIN_SCORE_RELAXED)
    Contoh: /setscoreupto 55 40
    Range : 0 (reset) atau 10 – 100. Tersimpan di state (aman saat restart).
    Catatan: SHORT di tier RELAXED tetap kena SHORT_SCORE_PENALTY_RELAXED.
    """
    global MIN_SCORE_CUSTOM, MIN_SCORE_RELAXED_CUSTOM

    def _status() -> str:
        if MIN_SCORE_CUSTOM > 0:
            return (f"FULL ≥<b>{MIN_SCORE_CUSTOM}</b> | "
                    f"RELAXED ≥<b>{MIN_SCORE_RELAXED_CUSTOM}</b> (custom)")
        return (f"FULL ≥<b>{MIN_SCORE}</b> | "
                f"RELAXED ≥<b>{MIN_SCORE_RELAXED}</b> (default)")

    if len(parts) < 2:
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan:\n"
            "<code>/setscoreupto 55 40</code>  → FULL 55, RELAXED 40\n"
            "<code>/setscoreupto 50</code>  → semua tier 50\n"
            "<code>/setscoreupto 0</code>  → reset ke default\n\n"
            f"Saat ini: {_status()}"
        )
        return

    try:
        vals = [int(x) for x in parts[1:3]]
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka bulat. Contoh: <code>/setscoreupto 55 40</code>")
        return

    if len(vals) == 1 and vals[0] == 0:
        MIN_SCORE_CUSTOM = 0
        MIN_SCORE_RELAXED_CUSTOM = 0
        save_state()
        send_telegram_raw(f"♻️ Score gate di-reset ke default.\n🧮 {_status()}")
        return

    if any(not (10 <= v <= 100) for v in vals):
        send_telegram_raw("⚠️ Skor harus antara <b>10</b> sampai <b>100</b> (atau <code>0</code> untuk reset).")
        return

    full_v = vals[0]
    relaxed_v = vals[1] if len(vals) > 1 else vals[0]
    old = _status()
    MIN_SCORE_CUSTOM = full_v
    MIN_SCORE_RELAXED_CUSTOM = relaxed_v
    save_state()
    send_telegram_raw(
        "✅ <b>Score gate diubah</b>\n"
        f"Sebelum : {old}\n"
        f"Sekarang: {_status()}\n"
        "ℹ️ SHORT di tier RELAXED tetap butuh skor mentah ≥ gate + "
        f"{SHORT_SCORE_PENALTY_RELAXED}."
    )


def _pairs_norm(tok: str):
    """BTC -> BTCUSDT, shib -> 1000SHIBUSDT (kalau ada di daftar bot)."""
    t = tok.strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if not t:
        return None
    if not t.endswith("USDT"):
        t += "USDT"
    if t in BASE_PAIRS:
        return t
    alt = "1000" + t
    return alt if alt in BASE_PAIRS else t


def _pairs_parse(tokens: list):
    """-> (pair valid urut sesuai BASE_PAIRS, token yang tidak dikenal)."""
    raw = []
    for a in tokens:
        raw += [x for x in a.replace(",", " ").split() if x.strip()]
    ok, bad = set(), []
    for r in raw:
        p = _pairs_norm(r)
        if p in BASE_PAIRS:
            ok.add(p)
        else:
            bad.append(r)
    return [p for p in BASE_PAIRS if p in ok], bad


def _pairs_status_text() -> str:
    shown = list(_PAIR_SELECTION) if _PAIR_SELECTION else list(BASE_PAIRS)
    total = len(BASE_PAIRS)
    if _PAIR_SELECTION:
        head = f"🎯 <b>TERPILIH</b> — {len(shown)} dari {total} pair"
    else:
        head = f"🌐 <b>SEMUA</b> — {total} pair"
    names = ", ".join(p.replace("USDT", "") for p in shown)
    try:
        n_valid = len(get_active_pairs())
    except Exception:
        n_valid = len(shown)
    return (
        f"📡 <b>Pair yang Dipantau</b>\n"
        f"{'─'*34}\n"
        f"{head}\n"
        f"{names}\n"
        f"{'─'*34}\n"
        f"✅ Dipindai di mode {BOT_MODE}: <b>{n_valid}</b> pair\n"
        f"ℹ️ Berlaku mulai putaran scan berikutnya. Posisi yang sudah terbuka tetap dikelola sampai selesai."
    )


def cmd_pairs(parts: list):
    """
    /pairs                    → lihat pair yang dipantau sekarang
    /pairs all                → pantau SEMUA pair
    /pairs 15                 → 15 pair pertama di daftar bot
    /pairs BTC,ETH,SOL        → hanya pair ini (nama pendek boleh: BTC, shib, pepe)
    /pairs add DOGE,SUI       → tambah pair ke pilihan sekarang
    /pairs remove DOGE        → keluarkan pair dari pilihan sekarang
    Tanpa restart; tersimpan di bot_state.json.
    """
    global _PAIR_SELECTION
    args = [a for a in parts[1:] if a.strip()]
    usage = (
        "\n\nCara pakai:\n"
        "<code>/pairs all</code> → semua pair\n"
        "<code>/pairs 15</code> → 15 pair pertama\n"
        "<code>/pairs BTC,ETH,SOL</code> → hanya pair ini\n"
        "<code>/pairs add DOGE</code> / <code>/pairs remove DOGE</code>"
    )
    if not args:
        send_telegram_raw(_pairs_status_text() + usage)
        return

    first = args[0].lower()
    total = len(BASE_PAIRS)

    if first in ("all", "semua", "reset"):
        new_sel = None
    elif first.isdigit():
        n = int(first)
        if not (1 <= n <= total):
            send_telegram_raw(f"⚠️ Jumlah pair harus antara <b>1</b> sampai <b>{total}</b>.")
            return
        new_sel = None if n == total else list(BASE_PAIRS)[:n]
    elif first in ("add", "tambah", "remove", "hapus", "del"):
        picked, bad = _pairs_parse(args[1:])
        if bad or not picked:
            send_telegram_raw(
                "⚠️ Pair tidak dikenal: <b>" + (", ".join(bad) if bad else "(kosong)") + "</b>\n"
                "Pair harus ada di daftar bot. Contoh: <code>/pairs add DOGE,SUI</code>"
            )
            return
        cur = list(_PAIR_SELECTION) if _PAIR_SELECTION else list(BASE_PAIRS)
        if first in ("add", "tambah"):
            cur = [p for p in BASE_PAIRS if p in set(cur) | set(picked)]
        else:
            cur = [p for p in cur if p not in set(picked)]
        if not cur:
            send_telegram_raw("⚠️ Minimal harus ada <b>1 pair</b> yang dipantau.")
            return
        new_sel = None if len(cur) == total else cur
    else:
        picked, bad = _pairs_parse(args)
        if bad or not picked:
            send_telegram_raw(
                "⚠️ Pair tidak dikenal: <b>" + (", ".join(bad) if bad else "(kosong)") + "</b>\n"
                "Pair harus ada di daftar bot. Contoh: <code>/pairs BTC,ETH,SOL</code>"
            )
            return
        new_sel = None if len(picked) == total else picked

    old_txt = f"{len(_PAIR_SELECTION)} pair" if _PAIR_SELECTION else "semua pair"
    _PAIR_SELECTION = new_sel
    save_state()
    new_txt = f"{len(new_sel)} pair" if new_sel else "semua pair"
    print(f"⚙️ /pairs diubah: {old_txt} → {new_txt}")
    send_telegram_raw(f"✅ <b>Pair Diubah</b>: {old_txt} → <b>{new_txt}</b>\n\n" + _pairs_status_text())


def cmd_maxopentrade(parts: list):
    """
    /maxopentrade <jumlah>
    Ubah MAX_OPEN_TRADES secara live tanpa restart bot.
    Contoh: /maxopentrade 3
    Range  : 1 – 20
    """
    global MAX_OPEN_TRADES
    if len(parts) < 2:
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/maxopentrade 3</code>\n"
            f"Max open trades saat ini: <b>{MAX_OPEN_TRADES}</b>\n"
            f"Posisi aktif sekarang   : <b>{len(active_positions)}</b>"
        )
        return

    try:
        new_max = int(parts[1])
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka bulat. Contoh: <code>/maxopentrade 3</code>")
        return

    if not (1 <= new_max <= 20):
        send_telegram_raw("⚠️ Jumlah posisi harus antara <b>1</b> sampai <b>20</b>.")
        return

    global _user_set_max_trades
    old_max      = MAX_OPEN_TRADES
    MAX_OPEN_TRADES = new_max
    _user_set_max_trades = True   # user sudah set manual → jadi hard cap
    active_count = len(active_positions)

    # Peringatan jika new_max lebih kecil dari posisi aktif saat ini
    warn = ""
    if new_max < active_count:
        warn = (
            f"\n⚠️ <b>Perhatian:</b> Posisi aktif ({active_count}) melebihi limit baru ({new_max}).\n"
            f"Bot tidak akan buka posisi baru sampai posisi aktif berkurang."
        )

    # Saran berdasarkan modal (dari dynamic tier)
    try:
        bal = get_total_balance()
        bal_info = f"💰 Balance saat ini: <b>${bal:.2f}</b>\n" if bal is not None else ""
    except Exception:
        bal_info = ""

    send_telegram_raw(
        f"✅ <b>Max Open Trades Diubah</b>\n"
        f"{'─'*34}\n"
        f"📉 Sebelum : <b>{old_max}</b> posisi\n"
        f"📈 Sekarang: <b>{new_max}</b> posisi\n"
        f"📌 Aktif   : <b>{active_count}</b> posisi\n"
        f"{'─'*34}\n"
        f"{bal_info}"
        f"ℹ️ Dynamic tier tetap aktif sebagai batas atas otomatis.{warn}"
    )
    print(f"⚙️ MAX_OPEN_TRADES diubah: {old_max} → {new_max}")
    save_state()













def cmd_maxtradesperday(parts: list):
    """
    /maxtradesperday <jumlah>
    Batas jumlah trade yang boleh DIBUKA per hari (reset 00:00 UTC).
    0 = tidak dibatasi. Contoh: /maxtradesperday 5
    Yang dihitung: order yang benar-benar TERISI. Limit order yang batal/expired
    tidak memakai jatah.
    """
    global MAX_TRADES_PER_DAY
    filled  = get_trades_today()
    pending = len(pending_limit_orders)

    if len(parts) < 2:
        cur = f"<b>{MAX_TRADES_PER_DAY}</b> / hari" if MAX_TRADES_PER_DAY > 0 else "<b>OFF</b> (tanpa batas)"
        send_telegram_raw(
            "⚠️ Format salah.\n"
            "Gunakan: <code>/maxtradesperday 5</code>  (0 = matikan batas)\n"
            f"Batas saat ini  : {cur}\n"
            f"Terisi hari ini : <b>{filled}</b>\n"
            f"Pending limit   : <b>{pending}</b>"
        )
        return

    try:
        new_max = int(parts[1])
    except ValueError:
        send_telegram_raw("⚠️ Masukkan angka bulat. Contoh: <code>/maxtradesperday 5</code>")
        return

    if not (0 <= new_max <= 200):
        send_telegram_raw("⚠️ Jumlah harus antara <b>0</b> (off) sampai <b>200</b>.")
        return

    old_max = MAX_TRADES_PER_DAY
    MAX_TRADES_PER_DAY = new_max
    old_s = f"{old_max}" if old_max > 0 else "OFF"
    new_s = f"{new_max}" if new_max > 0 else "OFF"

    warn = ""
    if new_max > 0 and filled >= new_max:
        warn = (
            f"\n⚠️ <b>Perhatian:</b> hari ini sudah terisi {filled} trade, "
            f"batas baru {new_max} → bot tidak buka trade baru sampai UTC 00:00."
        )
    elif new_max > 0 and (filled + pending) >= new_max:
        warn = (
            f"\nℹ️ Terisi {filled} + pending {pending} sudah menyentuh batas — "
            f"sinyal baru ditahan sampai ada slot."
        )

    send_telegram_raw(
        f"✅ <b>Max Trades Per Hari Diubah</b>\n"
        f"{'─'*34}\n"
        f"📉 Sebelum : <b>{old_s}</b>\n"
        f"📈 Sekarang: <b>{new_s}</b>\n"
        f"📌 Terisi hari ini: <b>{filled}</b> | Pending: <b>{pending}</b>\n"
        f"{'─'*34}\n"
        f"🕛 Reset otomatis tiap UTC 00:00.{warn}"
    )
    print(f"⚙️ MAX_TRADES_PER_DAY diubah: {old_s} → {new_s}")
    save_state()


def cmd_changeliveordemo():
    """
    /changeliveordemo
    Toggle antara mode LIVE (mainnet Binance) dan DEMO (testnet).
    LIVE  = uang asli — order masuk ke akun Binance mainnet.
    DEMO  = testnet   — order masuk ke testnet, tidak ada uang asli.
    """
    global BOT_MODE
    if BOT_MODE == "DEMO":
        BOT_MODE = "LIVE"
        url_now  = BASE_URL_LIVE
        warn = (
            "\n⚠️ <b>PERINGATAN:</b> Mode LIVE aktif!\n"
            "Semua order berikutnya masuk ke <b>akun Binance ASLI</b>.\n"
            "Pastikan API key yang kamu pakai adalah key mainnet, bukan testnet!"
        )
        em = "🔴"
    else:
        BOT_MODE = "DEMO"
        url_now  = BASE_URL_DEMO
        warn = "\nℹ️ Bot kembali ke mode testnet — tidak ada uang asli yang digunakan."
        em = "🟢"

    # ── Reset balance_start ke saldo mode yang baru ──────────────────────────
    # PENTING: wajib reset agar check_drawdown() tidak false-trigger.
    # Saldo DEMO (testnet) berbeda jauh dengan LIVE (mainnet).
    # FIX: reset JUGA mode_pnl[BOT_MODE]["start_bal"] ke saldo mode baru.
    # check_drawdown() sekarang pakai start_bal per mode sebagai baseline utama,
    # sehingga switch DEMO->LIVE (atau sebaliknya) tidak pernah false-trigger drawdown.
    try:
        new_bal = init_balance()
        # FIX: reset start_bal mode aktif ke saldo mode ini (bukan saldo mode lama)
        if new_bal and new_bal > 0:
            bot_state["balance_start"] = new_bal
            bot_state["mode_pnl"][BOT_MODE]["start_bal"] = new_bal
        bal_note = f"\n💰 Balance ({BOT_MODE}): <b>{new_bal:.2f} USDT</b>\n⚠️ Drawdown baseline di-reset ke saldo ini."
    except Exception as e:
        bot_state["balance_start"] = 0   # reset manual agar tidak false-trigger
        bot_state["mode_pnl"][BOT_MODE]["start_bal"] = 0.0
        bal_note = f"\n⚠️ Gagal fetch balance ({e}) — drawdown baseline di-reset."
        print(f"⚠️ Gagal reset balance saat ganti mode: {e}")

    # ── Reset daily limit state ke saldo mode baru ────────────────────────────
    # PENTING: balance_open HARUS di-reset saat ganti mode.
    # Jika tidak → PnL harian dihitung dari balance mode lama (DEMO vs LIVE beda jauh)
    # → limit langsung terpicu false-positive saat switch.
    try:
        now              = datetime.now(timezone.utc)
        today            = now.strftime("%Y-%m-%d")
        month_str        = now.strftime("%Y-%m")
        new_balance_open = get_total_balance()

        # FIX: get_total_balance() bisa return None saat API error (by design).
        # Jika None → fallback ke 0.0 agar tidak crash saat format string.
        if new_balance_open is None:
            raise Exception("get_total_balance() return None — API error atau 401 Unauthorized")

        # ── Reset daily limit baseline ─────────────────────────────────────────
        _daily_limit_state["date"]         = today
        _daily_limit_state["balance_open"] = new_balance_open
        _daily_limit_state["paused_by"]    = None
        _daily_limit_state["auto_started"] = False

        # ── Reset Cumulative PNL snapshot (Hari ini & Bulan ini) ───────────────
        # PENTING: saldo DEMO vs LIVE beda jauh → jika tidak di-reset,
        # Cumulative PNL langsung terhitung minus/plus ekstrem saat switch mode.
        bot_state["balance_day_start"]   = new_balance_open
        bot_state["balance_day_date"]    = today
        bot_state["balance_month_start"] = new_balance_open
        bot_state["balance_month_key"]   = month_str

        daily_reset_note = (
            f"\n📅 Daily limit & Cumulative PNL baseline di-reset:\n"
            f"   Balance open ({BOT_MODE}): <b>{new_balance_open:.2f} USDT</b>\n"
            f"   PnL harian & bulanan dihitung ulang dari 0 untuk mode ini."
        )
        print(f"📅 Daily limit + Cumulative PNL di-reset saat switch mode → {BOT_MODE} | balance_open={new_balance_open:.2f}")
    except Exception as e:
        _daily_limit_state["balance_open"] = 0.0
        _daily_limit_state["paused_by"]    = None
        daily_reset_note = f"\n⚠️ Gagal reset daily baseline ({e})."
        print(f"⚠️ Gagal reset daily limit state saat ganti mode: {e}")

    send_telegram_raw(
        f"{em} <b>Mode Bot Diubah</b>\n"
        f"{'─'*34}\n"
        f"⚡ Mode Sekarang : <b>{BOT_MODE}</b>\n"
        f"🌐 Endpoint      : <code>{url_now}</code>\n"
        f"{'─'*34}"
        f"{warn}"
        f"{bal_note}"
        f"{daily_reset_note}"
    )
    print(f"⚙️ BOT_MODE diubah → {BOT_MODE} | URL: {url_now}")
    save_state()

    # Kalau switch ke LIVE → deteksi IP + ingatkan whitelist di Binance
    if BOT_MODE == "LIVE":
        def _notify_ip_for_live():
            print("\n🌐 Switch ke LIVE — mendeteksi IP publik...")
            public_ip = get_public_ip()
            now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
            if public_ip:
                print(f"✅ IP terdeteksi: {public_ip}")
                msg = (
                    f"🌐 <b>IP Server — Cek Whitelist LIVE ({now_str})</b>\n"
                    f"{'─'*38}\n"
                    f"📡 IP Publik   : <code>{public_ip}</code>\n"
                    f"{'─'*38}\n"
                    f"⚠️ <b>Pastikan IP ini sudah di-whitelist</b> di API key mainnet Binance:\n"
                    f"  1. Buka <b>Binance → API Management</b>\n"
                    f"  2. Edit API key LIVE kamu\n"
                    f"  3. Tambahkan <code>{public_ip}</code> ke IP Restriction\n"
                    f"  4. Simpan perubahan\n"
                    f"{'─'*38}\n"
                    f"🔴 Bot sekarang aktif di mode <b>LIVE</b> — order masuk ke akun asli!"
                )
            else:
                print("❌ Gagal deteksi IP publik saat switch ke LIVE.")
                msg = (
                    f"⚠️ <b>Gagal Deteksi IP — Switch ke LIVE ({now_str})</b>\n"
                    f"{'─'*38}\n"
                    f"Tidak bisa mendeteksi IP publik server ini.\n"
                    f"🔧 Cek manual IP kamu dan whitelist di Binance API Management\n"
                    f"sebelum bot mulai order di mode LIVE!\n"
                    f"{'─'*38}\n"
                    f"🔴 Bot aktif di mode <b>LIVE</b> — pastikan IP sudah benar!"
                )
            send_telegram_raw(msg)
        threading.Thread(target=_notify_ip_for_live, daemon=True).start()


# ── Backtest State ────────────────────────────────────────────────────────────
_backtest_running = False
_backtest_proc = None











def _get_daily_pnl_pct() -> tuple[float, float, float]:
    """
    Hitung daily PnL % dari balance awal hari ini (UTC 00:00).
    Return: (pnl_usdt, pnl_pct, balance_open)
      pnl_usdt    = PnL hari ini dalam USDT
      pnl_pct     = PnL hari ini dalam persen dari balance_open
      balance_open= Balance awal hari ini
    FIX: jika get_total_balance() return None (API error / IP ban),
         return (0.0, 0.0, 0.0) agar check_daily_limits() tidak salah
         menghitung PnL = -100% dan memicu bot pause.
    """
    try:
        current = get_total_balance()
    except Exception:
        return 0.0, 0.0, 0.0

    # FIX: None = API error (bukan saldo 0) → skip perhitungan, jangan pause bot
    if current is None:
        print("  ⚠️  _get_daily_pnl_pct: balance None (API error) — skip daily limit check")
        return 0.0, 0.0, 0.0

    balance_open = _daily_limit_state.get("balance_open", 0.0)
    if balance_open <= 0:
        return 0.0, 0.0, 0.0

    pnl_usdt = current - balance_open
    pnl_pct  = (pnl_usdt / balance_open) * 100.0
    return pnl_usdt, pnl_pct, balance_open


def _reset_daily_limit_for_new_day():
    """
    Reset state daily limit untuk hari baru (UTC 00:00).
    Dipanggil otomatis di awal setiap hari baru.
    Auto-start bot hanya jika hari sebelumnya kena limit (DAILY_LOSS atau DAILY_WIN).
    """
    global bot_paused, _daily_limit_state

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _daily_limit_state["date"] == today:
        return   # sudah di-reset untuk hari ini

    paused_by_prev = _daily_limit_state.get("paused_by")

    # Snapshot balance awal hari baru
    try:
        new_balance_open = get_total_balance()
        # FIX: jika None (API error), pakai balance terakhir yang diketahui
        if new_balance_open is None:
            new_balance_open = _daily_limit_state.get("balance_open", 0.0) or 0.0
    except Exception:
        new_balance_open = _daily_limit_state.get("balance_open", 0.0)

    _daily_limit_state["date"]         = today
    _daily_limit_state["balance_open"] = new_balance_open
    _daily_limit_state["paused_by"]    = None
    _daily_limit_state["auto_started"] = False

    print(f"📅 Daily limit reset — {today} | Balance open: {new_balance_open:.2f} USDT")

    # ── Auto-start hanya jika kemarin kena limit (BUKAN pause manual) ────────
    if paused_by_prev in ("DAILY_LOSS", "DAILY_WIN", "DRAWDOWN") and bot_paused and not _daily_limit_state.get("period_pause_kind"):
        bot_paused = False
        _daily_limit_state["auto_started"] = True
        limit_label = {"DAILY_LOSS": "Daily Loss", "DAILY_WIN": "Daily Win", "DRAWDOWN": "Drawdown"}.get(paused_by_prev, "Limit")
        print(f"▶️ Bot auto-start: hari baru, limit {limit_label} kemarin sudah direset")
        send_telegram_raw(
            f"☀️ <b>Hari Baru — Bot Auto-Start</b>\n"
            f"{'─'*34}\n"
            f"📅 Tanggal     : <b>{today}</b>\n"
            f"💰 Balance baru: <b>{new_balance_open:.2f} USDT</b>\n"
            f"🔄 Kemarin kena <b>{limit_label} Limit</b> → otomatis di-resume.\n"
            f"{'─'*34}\n"
            f"{'🛑 Max Daily Loss: ' + str(DAILY_LOSS_LIMIT_PCT) + '%' if DAILY_LOSS_LIMIT_PCT > 0 else ''}\n"
            f"{'🎯 Max Daily Win : ' + str(DAILY_WIN_LIMIT_PCT) + '%' if DAILY_WIN_LIMIT_PCT > 0 else ''}\n"
            f"Bot scanning & trading aktif kembali!"
        )
    else:
        # Hari baru, bot tidak auto-start (pause manual atau limit tidak aktif)
        send_telegram_raw(
            f"☀️ <b>Hari Baru — Daily Limit Reset</b>\n"
            f"{'─'*34}\n"
            f"📅 Tanggal     : <b>{today}</b>\n"
            f"💰 Balance open: <b>{new_balance_open:.2f} USDT</b>\n"
            f"{'🛑 Max Daily Loss: ' + str(DAILY_LOSS_LIMIT_PCT) + '%' if DAILY_LOSS_LIMIT_PCT > 0 else '⭕ Daily Loss : OFF'}\n"
            f"{'🎯 Max Daily Win : ' + str(DAILY_WIN_LIMIT_PCT) + '%' if DAILY_WIN_LIMIT_PCT > 0 else '⭕ Daily Win  : OFF'}\n"
            f"{'─'*34}\n"
            f"{'▶️ Bot aktif.' if not bot_paused else '⏸ Bot masih PAUSE — kirim /start untuk mulai.'}"
        )


def check_daily_limits():
    """
    Cek apakah daily loss atau daily win limit sudah tercapai.
    Jika ya → pause bot otomatis dan kirim notifikasi.
    Dipanggil di main loop setiap iterasi.
    """
    global bot_paused, _daily_limit_state

    # Cek & reset hari baru dulu
    _reset_daily_limit_for_new_day()

    # Jika tidak ada limit yang aktif → tidak perlu cek
    if DAILY_LOSS_LIMIT_PCT <= 0 and DAILY_WIN_LIMIT_PCT <= 0:
        return

    # Jika bot sudah di-pause oleh limit hari ini → tidak perlu cek lagi
    if _daily_limit_state.get("paused_by") in ("DAILY_LOSS", "DAILY_WIN"):
        return

    pnl_usdt, pnl_pct, balance_open = _get_daily_pnl_pct()

    # ── Cek Daily Loss ────────────────────────────────────────────────────────
    if DAILY_LOSS_LIMIT_PCT > 0 and pnl_pct <= -DAILY_LOSS_LIMIT_PCT:
        bot_paused = True
        _daily_limit_state["paused_by"] = "DAILY_LOSS"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        msg = (
            f"🛑 <b>DAILY LOSS LIMIT TERCAPAI — Bot Di-PAUSE</b>\n"
            f"{'─'*38}\n"
            f"📅 Tanggal       : <b>{today}</b>\n"
            f"💰 Balance Open  : <b>{balance_open:.2f} USDT</b>\n"
            f"📉 PnL Hari Ini  : <b>{pnl_usdt:+.2f} USDT ({pnl_pct:+.2f}%)</b>\n"
            f"🛑 Limit Loss    : <b>-{DAILY_LOSS_LIMIT_PCT:.1f}%</b>\n"
            f"{'─'*38}\n"
            f"⏸ <b>Bot otomatis PAUSE sampai besok (UTC 00:00).</b>\n"
            f"🔄 Bot akan auto-start besok pagi.\n"
            f"ℹ️ Gunakan /resumeorpause jika ingin resume manual hari ini."
        )
        print(f"🛑 DAILY LOSS LIMIT: {pnl_pct:.2f}% ≤ -{DAILY_LOSS_LIMIT_PCT}% → bot PAUSED")
        send_telegram_raw(msg)
        return

    # ── Cek Daily Win ─────────────────────────────────────────────────────────
    if DAILY_WIN_LIMIT_PCT > 0 and pnl_pct >= DAILY_WIN_LIMIT_PCT:
        bot_paused = True
        _daily_limit_state["paused_by"] = "DAILY_WIN"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        msg = (
            f"🎯 <b>DAILY WIN LIMIT TERCAPAI — Bot Di-PAUSE</b>\n"
            f"{'─'*38}\n"
            f"📅 Tanggal       : <b>{today}</b>\n"
            f"💰 Balance Open  : <b>{balance_open:.2f} USDT</b>\n"
            f"📈 PnL Hari Ini  : <b>{pnl_usdt:+.2f} USDT ({pnl_pct:+.2f}%)</b>\n"
            f"🎯 Limit Win     : <b>+{DAILY_WIN_LIMIT_PCT:.1f}%</b>\n"
            f"{'─'*38}\n"
            f"⏸ <b>Bot otomatis PAUSE sampai besok (UTC 00:00).</b>\n"
            f"🔄 Bot akan auto-start besok pagi.\n"
            f"ℹ️ Gunakan /resumeorpause jika ingin resume manual hari ini."
        )
        print(f"🎯 DAILY WIN LIMIT: {pnl_pct:.2f}% ≥ +{DAILY_WIN_LIMIT_PCT}% → bot PAUSED")
        send_telegram_raw(msg)
        return





def _period_keys() -> tuple[str, str]:
    """(week_id, month_id) UTC. Minggu mulai Senin 00:00 UTC (ISO week)."""
    n = datetime.now(timezone.utc)
    iso = n.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}", n.strftime("%Y-%m")


def check_period_limits():
    """
    Limit MINGGUAN (loss) dan BULANAN (loss + profit target). Dipanggil tiap iterasi setelah
    check_daily_limits(). Kena limit → bot_paused=True sampai periode berikutnya, lalu auto-start.
    State disimpan di _daily_limit_state (ikut tersimpan ke bot_state.json).
    """
    global bot_paused

    if WEEKLY_LOSS_LIMIT_PCT <= 0 and MONTHLY_LOSS_LIMIT_PCT <= 0 and MONTHLY_WIN_LIMIT_PCT <= 0:
        return
    try:
        cur = get_total_balance()
    except Exception:
        return
    if cur is None or cur <= 0:
        return   # API error → jangan pause

    st = _daily_limit_state
    wk, mo = _period_keys()

    # ── Periode baru: snapshot balance awal + auto-resume kalau pause-nya milik periode lama ──
    resumed = None
    if st.get("week_id") != wk:
        st["week_id"] = wk
        st["week_balance_open"] = cur
        if st.get("period_pause_kind") == "WEEKLY_LOSS":
            resumed = "Weekly Loss"
            st["period_pause_kind"] = None
    if st.get("month_id") != mo:
        st["month_id"] = mo
        st["month_balance_open"] = cur
        if st.get("period_pause_kind") in ("MONTHLY_LOSS", "MONTHLY_WIN"):
            resumed = "Monthly Loss" if st.get("period_pause_kind") == "MONTHLY_LOSS" else "Monthly Win"
            st["period_pause_kind"] = None
    if resumed and bot_paused:
        bot_paused = False
        print(f"▶️ Bot auto-start: periode baru, limit {resumed} sudah direset")
        send_telegram_raw(
            f"☀️ <b>Periode Baru — Bot Auto-Start</b>\n"
            f"{'─'*34}\n"
            f"🔄 Limit <b>{resumed}</b> sebelumnya sudah direset.\n"
            f"💰 Balance open minggu: <b>{st.get('week_balance_open', 0):.2f}</b> | bulan: <b>{st.get('month_balance_open', 0):.2f}</b> USDT\n"
            f"Bot scanning & trading aktif kembali!"
        )

    if st.get("period_pause_kind"):
        return   # sudah di-pause oleh limit periode ini

    wo = st.get("week_balance_open") or 0.0
    mob = st.get("month_balance_open") or 0.0
    wpct = (cur - wo) / wo * 100.0 if wo > 0 else 0.0
    mpct = (cur - mob) / mob * 100.0 if mob > 0 else 0.0

    kind = None
    if WEEKLY_LOSS_LIMIT_PCT > 0 and wpct <= -WEEKLY_LOSS_LIMIT_PCT:
        kind, title, pct, lim, until = "WEEKLY_LOSS", "🛑 WEEKLY LOSS LIMIT TERCAPAI", wpct, f"-{WEEKLY_LOSS_LIMIT_PCT:.1f}%", "Senin depan (UTC 00:00)"
    elif MONTHLY_LOSS_LIMIT_PCT > 0 and mpct <= -MONTHLY_LOSS_LIMIT_PCT:
        kind, title, pct, lim, until = "MONTHLY_LOSS", "🛑 MONTHLY LOSS LIMIT TERCAPAI", mpct, f"-{MONTHLY_LOSS_LIMIT_PCT:.1f}%", "tanggal 1 bulan depan (UTC 00:00)"
    elif MONTHLY_WIN_LIMIT_PCT > 0 and mpct >= MONTHLY_WIN_LIMIT_PCT:
        kind, title, pct, lim, until = "MONTHLY_WIN", "🎯 MONTHLY PROFIT TARGET TERCAPAI", mpct, f"+{MONTHLY_WIN_LIMIT_PCT:.1f}%", "tanggal 1 bulan depan (UTC 00:00)"

    if kind:
        bot_paused = True
        st["period_pause_kind"] = kind
        print(f"{title}: {pct:+.2f}% (limit {lim}) → bot PAUSED sampai {until}")
        send_telegram_raw(
            f"{title} — Bot Di-PAUSE\n"
            f"{'─'*38}\n"
            f"💰 Balance Sekarang : <b>{cur:.2f} USDT</b>\n"
            f"📊 PnL Periode      : <b>{pct:+.2f}%</b> (limit {lim})\n"
            f"{'─'*38}\n"
            f"⏸ <b>Bot otomatis PAUSE sampai {until}.</b>\n"
            f"🔄 Bot akan auto-start di periode berikutnya.\n"
            f"ℹ️ Gunakan /resumeorpause jika ingin resume manual."
        )


def send_message_with_keyboard(text: str, keyboard: list):
    """
    Kirim pesan Telegram dengan inline keyboard buttons.
    keyboard = list of rows, tiap row = list of dict {text, callback_data}
    Contoh:
      [[{"text": "▶️ Resume", "callback_data": "/resumeorpause"}],
       [{"text": "💹 PnL", "callback_data": "/pnl"}, {"text": "📊 Status", "callback_data": "/status"}]]
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    reply_markup = {"inline_keyboard": keyboard}
    try:
        r = requests.post(url, json={
            "chat_id":      TELEGRAM_CHAT_ID,
            "text":         text,
            "parse_mode":   "HTML",
            "reply_markup": reply_markup,
        }, timeout=10)
        if not r.ok:
            # fallback plain text tanpa keyboard
            send_telegram_raw(text)
    except Exception as e:
        print(f"⚠️ send_message_with_keyboard error: {e}")
        send_telegram_raw(text)


def answer_callback_query(callback_query_id: str, text: str = ""):
    """Jawab callback query agar tombol tidak loading terus."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery"
    try:
        requests.post(url, json={
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": False,
        }, timeout=5)
    except Exception:
        pass




def cmd_togglestochgate():
    """Toggle hard gate Stochastic (divergence + anti-berlawanan) per sinyal. Nama fungsi/command dipertahankan untuk kompatibilitas."""
    global STOCH_POSITION_GATE_ENABLED
    STOCH_POSITION_GATE_ENABLED = not STOCH_POSITION_GATE_ENABLED

    if STOCH_POSITION_GATE_ENABLED:
        msg = (
            f"✅ <b>Stochastic Gate: ON</b>\n"
            f"{'─'*30}\n"
            f"HARD SKIP kalau Stoch (pair ATAU BTC) sudah mentok "
            f"(OB &gt;{STOCH_OVERBOUGHT} / OS &lt;{STOCH_OVERSOLD}) dan mulai\n"
            f"crossing balik ke arah LAWAN sinyal. Stoch netral/tengah → tetap lolos.\n"
            f"Divergence (pair/BTC) cuma info prioritas, TIDAK wajib ada.\n"
            f"Ketik /togglestochgate untuk matikan."
        )
    else:
        msg = (
            f"🔕 <b>Stochastic Gate: OFF</b>\n"
            f"{'─'*30}\n"
            f"Filter anti-berlawanan Stochastic dinonaktifkan.\n"
            f"Ketik /togglestochgate untuk aktifkan lagi."
        )
    send_telegram_raw(msg)
    print(f"{'✅ STOCH GATE ON' if STOCH_POSITION_GATE_ENABLED else '🔕 STOCH GATE OFF'}")
    save_state()


def cmd_menu():
    """
    Kirim menu tombol (inline keyboard) — semua command yang TIDAK butuh
    parameter tambahan, tinggal tap tanpa perlu ngetik apa-apa.
    Command yang butuh angka/teks (mis. /setmarginratio, /maxopentrade,
    /exportlog) tetap harus diketik manual karena butuh nilai.
    """
    keyboard = [
        [{"text": "▶️ Start", "callback_data": "/start"},
         {"text": "⏯ Resume/Pause", "callback_data": "/resumeorpause"}],
        [{"text": "💹 PnL", "callback_data": "/pnl"},
         {"text": "📊 Status", "callback_data": "/status"}],
        [{"text": "🔴 Close ALL", "callback_data": "/closeallposition"}],
        [{"text": "🟢 Close Profit", "callback_data": "/closeallprofit"},
         {"text": "🟥 Close Loss", "callback_data": "/closealllosing"}],
        [{"text": "🔄 LIVE ↔ DEMO", "callback_data": "/changeliveordemo"},
         {"text": "♻️ Reset PnL", "callback_data": "/resetpnl"}],
        [{"text": "📈 Toggle Stoch Gate", "callback_data": "/togglestochgate"}],
        [{"text": "📊 Recap", "callback_data": "/recap"}],
        [{"text": "🧹 Cleanup Orders", "callback_data": "/cleanuporders"},
         {"text": "🧹 Cleanup Duplicate", "callback_data": "/cleanupduplicate"}],
        [{"text": "📡 Pairs", "callback_data": "/pairs"}],
        [{"text": "❓ Help", "callback_data": "/help"}],
    ]
    send_message_with_keyboard("🤖 <b>Menu Bot</b> — tap tombol di bawah:", keyboard)


def _backtest_build_cmd(days: int, pairs: str, out_dir: str, end: str = "", rand_seed=None, extra=None) -> list:
    import sys as _sys
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest.py")
    cmd = [_sys.executable, script, "--days", str(days), "--pairs", pairs,
           "--out", out_dir, "--tag", "tg"]
    if end:
        cmd += ["--end", end]
    # 2026-11-17: backtest jalan di proses terpisah & tidak baca setting live (/maxopentrade) →
    # sebelumnya selalu pakai MAX_OPEN=1 dari backtest.py. Sekarang ikut MAX_OPEN_TRADES live.
    if "--max-open" not in (extra or []):
        cmd += ["--max-open", str(int(MAX_OPEN_TRADES))]
    if rand_seed is not None:
        cmd += ["--random-entry", "--seed", str(int(rand_seed))]
    if extra:
        cmd += list(extra)
    return cmd


def _backtest_send_result(summary_path: str, csv_path: str):
    import html as _html
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        send_telegram_raw("<pre>" + _html.escape(txt[:3800]) + "</pre>")
    except Exception as e:
        send_telegram_raw(f"⚠️ Gagal baca ringkasan backtest: <code>{e}</code>")
    try:
        with open(csv_path, "rb") as f:
            data = f.read()
        send_telegram_document(data, os.path.basename(csv_path),
                               caption="📄 Log backtest (TERPISAH dari trade_log live)")
    except Exception as e:
        send_telegram_raw(f"⚠️ Gagal kirim CSV backtest: <code>{e}</code>")


_trend_running = False


def cmd_trend(parts):
    """/trend [tahun=5] — jalankan trend_research.py (uji trend-following harian multi-tahun) di proses
    TERPISAH; hasil dikirim ke Telegram. Tidak menyentuh trade_log.json / bot_state.json / order Binance."""
    global _trend_running
    import html as _html
    import subprocess as _sp
    import sys as _sys
    import threading as _th
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "trend_research.py")
    if not os.path.exists(script):
        send_telegram_raw("❌ File <code>trend_research.py</code> tidak ada di server. Upload bersama main.py.")
        return
    if _trend_running:
        send_telegram_raw("⏳ /trend masih berjalan. Tunggu selesai.")
        return
    try:
        years = int(parts[1]) if len(parts) > 1 else 5
    except ValueError:
        send_telegram_raw("❌ Format: <code>/trend [tahun 1-8]</code>  contoh: <code>/trend 5</code>")
        return
    years = max(1, min(years, 8))
    send_telegram_raw(f"⏳ Menjalankan uji trend-following harian ({years} tahun, 8 coin). Biasanya 1–3 menit…")

    def _run():
        global _trend_running
        _trend_running = True
        try:
            r = _sp.run([_sys.executable, script, "--years", str(years)],
                        capture_output=True, text=True, timeout=1200)
            out = (r.stdout or "").strip() or (r.stderr or "").strip() or "(tidak ada output)"
            if r.returncode != 0:
                out = "❌ Gagal (kode %s)\n" % r.returncode + out[-1500:]
            send_telegram_raw("<pre>" + _html.escape(out[-3800:]) + "</pre>")
        except _sp.TimeoutExpired:
            send_telegram_raw("⚠️ /trend timeout (20 menit).")
        except Exception as e:  # noqa: BLE001
            send_telegram_raw(f"⚠️ /trend error: <code>{_html.escape(str(e))}</code>")
        finally:
            _trend_running = False

    _th.Thread(target=_run, daemon=True).start()


def cmd_backtest(parts):
    """
    /backtest [hari] [pair]   — jalankan backtest strategi di data historis Binance.
      /backtest               → 7 hari, 15 pair pertama
      /backtest 14 all        → 14 hari, semua pair
      /backtest 7 BTCUSDT,ETHUSDT
      /backtest 30 live sz1   → pair SAMA dengan yang dipantau live (lihat /pairs)
      /backtest 30 15 2026-10-12   → 30 hari SEBELUM 12 Okt 2026 (UTC)
      /backtest 60 15 2026-10-11 rand2 → BASELINE ACAK (seed 2): entry acak tanpa sinyal SMC,
                                         exit/fee/risk sama. Pembanding "apakah sinyal bot punya edge?"
      /backtest 60 15 2026-10-11 smc2     → STRATEGI BARU smc2.py (sweep→MSS→FVG/OB→target liquidity)
      /backtest 60 15 2026-10-11 smc2flip → KONTROL smc2: waktu/SL/R sama, ARAH diundi
      /backtest 60 15 2026-10-11 smc2:h4  → smc2 + filter bias 4H   (filter: h4 | btc | adx, gabung dgn +, mis. smc2:h4+btc)
      /backtest 60 15 2026-10-11 smc2flip:h4 → kontrol coinflip dgn filter yang sama
      /backtest 60 15 2026-10-11 ob       → STRATEGI order block (ob.py); obflip = kontrol coinflip
      /backtest 60 15 2026-10-11 fund     → STRATEGI kontrarian funding rate (fund.py); fundflip = kontrol coinflip
      /backtest 60 15 2026-10-11 htf      → STRATEGI tren 4H + pullback 1H (htf.py); htfflip = kontrol coinflip
      /backtest 60 15 2026-10-11 sd       → STRATEGI demand/supply zone (sd.py); sdflip = kontrol coinflip
      /backtest 60 15 2026-10-11 randsmc  → acak dgn struktur exit mirip smc2 (TP1 1R, TP2 3R, SL 1.5xATR)
      /backtest 30 15 dz1  → LONG di demand 1D + reaksi buyer (tanpa BOS) | dz2 = demand 1W
      /backtest 30 15 sz1  → SHORT di supply (OB merah LuxAlgo) 1H + reaksi seller (tanpa BOS) | sz2=4H sz3=1D sz4=1W
      /backtest 30 15 sz1 sg0 → sama, tapi pengaman SHORT (BTC naik / tren 1h melawan) DIMATIKAN, untuk pembanding
    /backtest status | stop | last
    Berjalan sebagai proses TERPISAH (backtest.py) — tidak menyentuh trade_log.json,
    bot_state.json, atau order Binance. Log tersimpan di <data_dir>/backtests/.
    """
    global _backtest_running, _backtest_proc
    import subprocess as _sp
    import threading as _th

    out_dir = os.path.join(_get_data_dir(), "backtests")
    sub = parts[1].lower() if len(parts) > 1 else ""

    if sub in ("stop", "cancel", "batal"):
        if _backtest_running and _backtest_proc is not None:
            try:
                _backtest_proc.terminate()
            except Exception:
                pass
            send_telegram_raw("🛑 Backtest dihentikan.")
        else:
            send_telegram_raw("ℹ️ Tidak ada backtest yang sedang berjalan.")
        return

    if sub == "status":
        send_telegram_raw("⏳ Backtest sedang berjalan." if _backtest_running
                          else "ℹ️ Tidak ada backtest yang sedang berjalan.")
        return

    if sub in ("last", "terakhir"):
        try:
            sums = sorted(f for f in os.listdir(out_dir)
                          if f.startswith("backtest_summary_") and f.endswith(".txt"))
        except Exception:
            sums = []
        if not sums:
            send_telegram_raw("ℹ️ Belum ada hasil backtest tersimpan.")
            return
        sp = os.path.join(out_dir, sums[-1])
        cp = os.path.join(out_dir, sums[-1].replace("backtest_summary_", "backtest_log_").replace(".txt", ".csv"))
        _backtest_send_result(sp, cp)
        return

    if _backtest_running:
        send_telegram_raw("⏳ Backtest masih berjalan. Cek <code>/backtest status</code> atau <code>/backtest stop</code>.")
        return
    if not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest.py")):
        send_telegram_raw("❌ File <code>backtest.py</code> tidak ada di server. Upload bersama main.py.")
        return

    days, pairs, end, rand_seed = 7, "15", "", None
    extra_args, mode_label = [], ""
    try:
        if len(parts) > 1:
            days = int(parts[1])
        if len(parts) > 2:
            pairs = parts[2]
            if pairs.lower() == "live":
                pairs = ",".join(get_active_pairs())   # pair yang sama dengan yang dipantau bot (/pairs)
        if len(parts) > 3:
            end = parts[3].strip()
            import re as _re
            if not _re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
                raise ValueError("end")
        if len(parts) > 4:
            _tok = parts[4].strip().lower()
            # Token ke-5 boleh langsung opsi (sr1.2 / pb / dl5 / dw5 / mt3) tanpa
            # harus isi mode strategi — mode default (SMC bot) dipakai.
            if _re.fullmatch(r"pb|flip|(?:sr|be)\d+(?:\.\d+)?|(?:dl|dw|mt|wl|ml|mw|rk|sf|sb|rz|rs|zs|zp|rg|dz|sz|sg)\d+(?:\.\d+)?", _tok):
                parts = list(parts[:4]) + ["-"] + list(parts[4:])
                _tok = "-"
            _m = _re.fullmatch(r"(rand|randsmc)(\d*)", _tok)
            if _tok == "-":
                pass   # mode default: strategi SMC bot
            elif _m:
                rand_seed = int(_m.group(2)) if _m.group(2) else 1
                mode_label = f"ACAK seed {rand_seed}"
                if _m.group(1) == "randsmc":
                    extra_args = ["--rand-sl-atr", "1.5", "--rand-tp1-r", "1.0", "--rand-tp2-r", "3.0"]
                    mode_label += " (exit mirip smc2)"
            elif _tok in ("fund", "fundflip"):
                extra_args, mode_label = ["--strategy", "fund"], "FUND kontrarian funding"
                if _tok == "fundflip":
                    extra_args.append("--coinflip")
                    mode_label = "FUND KONTROL coinflip"
            elif _tok in ("htf", "htfflip"):
                extra_args, mode_label = ["--strategy", "htf"], "HTF tren 4H pullback"
                if _tok == "htfflip":
                    extra_args.append("--coinflip")
                    mode_label = "HTF KONTROL coinflip"
            elif _tok in ("ob", "obflip"):
                extra_args, mode_label = ["--strategy", "ob"], "OB order block"
                if _tok == "obflip":
                    extra_args.append("--coinflip")
                    mode_label = "OB KONTROL coinflip"
            elif _tok.split(":")[0] in ("sd", "sdflip"):
                if ":" in _tok:
                    raise ValueError("mode")
                extra_args, mode_label = ["--strategy", "sd"], "SD demand/supply"
                if _tok == "sdflip":
                    extra_args.append("--coinflip")
                    mode_label = "SD KONTROL coinflip"
            elif _tok.split(":")[0] in ("smc2", "smc2flip"):
                _base, _, _fl = _tok.partition(":")
                extra_args, mode_label = ["--strategy", "smc2"], "SMC v2"
                if _base == "smc2flip":
                    extra_args.append("--coinflip")
                    mode_label = "SMC v2 KONTROL coinflip"
                _fmap = {"h4": "use_h4=1", "btc": "use_btc=1", "adx": "adx_min=20"}
                _used = []
                for _f in [x for x in _fl.split("+") if x]:
                    if _f not in _fmap:
                        raise ValueError("mode")
                    extra_args += ["--smc2-p", _fmap[_f]]
                    _used.append(_f)
                if _used:
                    mode_label += " [" + "+".join(_used) + "]"
            else:
                raise ValueError("mode")
        if len(parts) > 5:
            for _x in parts[5:]:
                _sm = _re.fullmatch(r"sr(\d+(?:\.\d+)?)", _x.strip().lower())
                if _sm:
                    extra_args += ["--single-tp", _sm.group(1)]
                    mode_label += f" sr{_sm.group(1)}"
                    continue
                _bm = _re.fullmatch(r"be(\d+(?:\.\d+)?)", _x.strip().lower())
                if _bm:
                    extra_args += ["--be-at-r", _bm.group(1)]
                    mode_label += f" BE@{_bm.group(1)}R"
                    continue
                if _x.strip().lower() == "flip":
                    extra_args += ["--flip"]
                    mode_label += " FLIP"
                    continue
                if _x.strip().lower() == "pb":
                    extra_args += ["--ema-pullback", "gate"]
                    mode_label += " pb"
                    continue
                _lm = _re.fullmatch(r"(dl|dw|mt|wl|ml|mw|rk|sf|sb|rz|rs|zs|zp|rg|dz|sz|sg)(\d+(?:\.\d+)?)", _x.strip().lower())
                if not _lm:
                    raise ValueError("limit")
                _flag = {"dl": "--daily-loss-pct", "dw": "--daily-win-pct", "mt": "--max-trades-day", "wl": "--weekly-loss-pct", "ml": "--monthly-loss-pct", "mw": "--monthly-win-pct", "rk": "--risk", "sf": "--struct-filter", "sb": "--sl-buffer", "rz": "--retest-zone", "rs": "--research", "zs": "--zone-src", "zp": "--zone-pick", "rg": "--regime-aligned", "dz": "--demand-long", "sz": "--supply-short", "sg": "--supply-guard"}[_lm.group(1)]
                _val = _lm.group(2)
                if _lm.group(1) == "mt":
                    _val = str(int(float(_val)))
                extra_args += [_flag, _val]
                mode_label += f" {_lm.group(1)}{_lm.group(2)}"
    except ValueError:
        send_telegram_raw("❌ Format: <code>/backtest [hari] [all|jumlah|BTCUSDT,ETHUSDT] [YYYY-MM-DD akhir periode] [rand|rand2|randsmc|smc2|smc2flip|smc2:h4|smc2:btc|smc2:adx|sd|sdflip|ob|obflip|htf|htfflip|fund|fundflip] [pb sr<R> dl<%> dw<%> mt<n> dz1 sz1 sg0]</code>")
        return
    days = max(1, min(days, 90))   # 2026-11-17: 60 → 90 (batas backtest.py = 90)
    cmd = _backtest_build_cmd(days, pairs, out_dir, end, rand_seed, extra_args)

    def _run():
        global _backtest_running, _backtest_proc
        _backtest_running = True
        csv_path = sum_path = None
        tail, sent = [], set()
        try:
            env = dict(os.environ)
            env["PYTHONIOENCODING"] = "utf-8"
            proc = _sp.Popen(cmd, stdout=_sp.PIPE, stderr=_sp.STDOUT, text=True,
                             encoding="utf-8", errors="replace",
                             cwd=os.path.dirname(os.path.abspath(__file__)), env=env)
            _backtest_proc = proc
            for line in proc.stdout:
                line = line.rstrip()
                tail = (tail + [line])[-12:]
                if line.startswith("PROGRESS "):
                    try:
                        pct = int(line.split()[1])
                    except Exception:
                        continue
                    for mark in (25, 50, 75):
                        if pct >= mark and mark not in sent:
                            sent.add(mark)
                            send_telegram_raw(f"⏳ Backtest {pct}% — {' '.join(line.split()[2:])}")
                elif line.startswith("CSV_FILE "):
                    csv_path = line[len("CSV_FILE "):].strip()
                elif line.startswith("SUMMARY_FILE "):
                    sum_path = line[len("SUMMARY_FILE "):].strip()
            rc = proc.wait()
            if rc == 0 and csv_path and sum_path:
                _backtest_send_result(sum_path, csv_path)
            elif rc in (-15, 15, -9):
                pass   # dihentikan manual
            else:
                import html as _html
                send_telegram_raw("❌ <b>Backtest gagal</b> (kode " + str(rc) + ")\n<pre>"
                                  + _html.escape("\n".join(tail)[-1500:]) + "</pre>")
        except Exception as e:
            send_telegram_raw(f"❌ Backtest error: <code>{e}</code>")
        finally:
            _backtest_running = False
            _backtest_proc = None

    send_telegram_raw(
        f"🧪 <b>Backtest dimulai</b>\n"
        f"Periode: {days} hari | Pair: {pairs}"
        + (f" | MODE {mode_label}" if mode_label else "") + "\n"
        f"Berjalan terpisah dari trading live — log TIDAK bercampur dengan trade_log.\n"
        f"Bisa lama (menit). Perintah: <code>/backtest status</code>, <code>/backtest stop</code>."
    )
    _th.Thread(target=_run, daemon=True).start()


def cmd_getcode():
    """
    /getcode — kirim file source bot yang SEDANG JALAN di Railway ke Telegram
    (cadangan kalau GitHub tidak bisa dibuka). Isi file = persis yang ter-deploy.
    Secret (API key/token) aman: semuanya dari env var, tidak ada di source.
    """
    try:
        _path = os.path.abspath(__file__)
        with open(_path, "rb") as _f:
            _data = _f.read()
    except Exception as e:
        send_telegram_raw(f"❌ Gagal baca file source: <code>{e}</code>")
        return

    _v = _bot_version_fields()
    _caption = (
        f"📄 <b>main.py</b> (yang sedang jalan di Railway)\n"
        f"🏷 {_v['bot_version']} | upload {_v['build_date']}\n"
        f"🐙 {_v['github_account']}/{_v['github_repo'] or '-'} "
        f"({_v['git_branch'] or '-'} @ {_v['git_sha'] or '-'})\n"
        f"📦 {len(_data)/1024:.0f} KB"
    )
    ok = send_telegram_document(_data, "main.py", caption=_caption)
    if not ok:
        send_telegram_raw("❌ Gagal kirim file ke Telegram, cek log Railway.")


def handle_command(text: str):
    """Dispatch perintah Telegram ke handler yang sesuai (versi simpel)."""
    text  = text.strip()
    parts = text.split()
    cmd   = parts[0].lower().split("@")[0]  # hapus @botname jika ada

    if cmd == "/start":
        cmd_start()
    elif cmd == "/pnl":
        cmd_pnl()
    elif cmd == "/status":
        cmd_status()
    elif cmd == "/resumeorpause":
        cmd_resumeorpause()
    elif cmd in ("/closeallposition", "/closeallpositions"):
        cmd_closeallposition()
    elif cmd in ("/closealllosing", "/closealllosses", "/closeloss", "/closelossposition"):
        cmd_closealllosing()
    elif cmd in ("/closeallprofit", "/closeallprofits", "/closeprofit", "/closeprofitposition"):
        cmd_closeallprofit()
    elif cmd == "/menu":
        cmd_menu()
    elif cmd in ("/getcode", "/getmain", "/sourcecode"):
        cmd_getcode()
    elif cmd in ("/backtest", "/bt"):
        cmd_backtest(parts)
    elif cmd == "/trend":
        cmd_trend(parts)
    elif cmd == "/setmarginratio":
        cmd_setmarginratio(parts)
    elif cmd == "/maxopentrade":
        cmd_maxopentrade(parts)
    elif cmd == "/pairs":
        cmd_pairs(parts)
    elif cmd in ("/setscoreupto", "/setscore"):
        cmd_setscoreupto(parts)
    elif cmd in ("/maxtradesperday", "/maxtradeperday", "/maxtradeday"):
        cmd_maxtradesperday(parts)
    elif cmd == "/changeliveordemo":
        cmd_changeliveordemo()
    elif cmd in ("/togglestochgate", "/togglestoch"):
        cmd_togglestochgate()
    elif cmd == "/resetpnl":
        cmd_resetpnl()
    elif cmd == "/resetstate":
        cmd_resetstate(parts)
    elif cmd in ("/exportlog", "/tradelog", "/downloadlog"):
        cmd_exportlog(parts)
    elif cmd in ("/dedupelog", "/cleanlog"):
        cmd_dedupelog(parts)
    elif cmd in ("/recap", "/rekap", "/summary"):
        cmd_recap(parts)
    elif cmd == "/resetlog":
        cmd_resetlog(parts)
    elif cmd in ("/clearcooldown", "/cleancooldown", "/cooldownclear"):
        cmd_clearcooldown(parts)
    elif cmd in ("/market", "/regime", "/breadth"):
        cmd_market(parts)
    elif cmd in ("/scorewr", "/scoreanalysis", "/wrbyscoreset"):
        cmd_scorewr(parts)
    elif cmd in ("/scorebd", "/scorebreakdown", "/wrbycomponent"):
        cmd_scorebd(parts)
    elif cmd in ("/cancelorders", "/cancelorder"):
        cmd_cancelorders(parts)
    elif cmd in ("/cleanuporders", "/cleanuporder", "/cleanup"):
        pending_info = (
            f"\n⏳ Pending limit orders ({len(pending_limit_orders)}): "
            f"{sorted(pending_limit_orders.keys()) or 'none'}"
        ) if pending_limit_orders else ""
        send_telegram_raw(
            f"🔄 <b>Cleanup orphan orders dimulai...</b>{pending_info}\n"
            f"Pending limit orders TIDAK akan disentuh."
        )
        try:
            cleanup_stale_orders(dry_run=False)
        except Exception as e:
            send_telegram_raw(f"❌ Cleanup error: <code>{e}</code>")
    elif cmd == "/cleanupduplicate":
        confirm = len(parts) > 1 and parts[1].lower() == "confirm"
        try:
            cleanup_duplicate_position_orders(dry_run=not confirm)
        except Exception as e:
            send_telegram_raw(f"❌ Cleanup duplicate error: <code>{e}</code>")
    elif cmd == "/help":
        sep = "─" * 34
        send_telegram_raw(
            f"🤖 <b>Daftar Command Bot</b>\n"
            f"{sep}\n"
            "/start\n"
            "  Mulai bot (cek koneksi Binance dulu, lalu sync posisi & trading)\n\n"
            "/pnl\n"
            "  Lihat unrealized PnL semua posisi aktif + cumulative PnL\n\n"
            "/recap [hari]\n"
            "  Recap performa manual (gaya 'SEMUA WAKTU') — sistem poin\n"
            "  TP2=1pt/TP1=1pt/SL=0pt + 10 trade terakhir. Tidak otomatis,\n"
            "  hanya terkirim saat diminta. Contoh: /recap atau /recap 30\n\n"
            "/status\n"
            "  Status lengkap bot + posisi aktif\n\n"
            "/resumeorpause\n"
            "  Toggle pause/resume scanning & trading\n\n"
            "/closeallposition\n"
            "  Tutup semua posisi aktif sekarang (market close)\n\n"
            "/closeallprofit\n"
            "  Tutup HANYA posisi yang lagi profit (unrealized PnL > 0)\n\n"
            "/closealllosing\n"
            "  Tutup HANYA posisi yang lagi loss (unrealized PnL < 0)\n\n"
            "/menu\n"
            "  Tampilkan menu tombol — command tanpa parameter tinggal tap\n\n"
            "/cancelorders SYMBOL\n"
            "  Cancel SEMUA order (termasuk TP/SL conditional) untuk 1 pair,\n"
            "  instan & tanpa syarat — beda dari /cleanuporders yang hanya\n"
            "  urus symbol tanpa posisi. Contoh: /cancelorders HYPEUSDT\n\n"
            "/setmarginratio &lt;persen&gt;\n"
            "  Set max SL loss % per trade — kerugian maks jika SL kena.\n"
            "  Lot dihitung mundur: lot = (balance × %) ÷ jarak_SL\n"
            "  <code>/setmarginratio 1</code>  → jika SL kena, max rugi 1% dari balance\n\n"
            "/setscoreupto &lt;full&gt; [relaxed]\n"
            "  Ubah minimal skor sinyal (FULL &amp; RELAXED) tanpa restart.\n"
            "  <code>/setscoreupto 55 40</code> → FULL 55, RELAXED 40\n"
            "  <code>/setscoreupto 0</code> → reset ke default\n\n"
            "/maxopentrade &lt;jumlah&gt;\n"
            "  Ubah max posisi yang boleh dibuka bersamaan (1–20), contoh:\n"
            "  <code>/maxopentrade 3</code>\n\n"
            "/pairs\n"
            "  Atur pair yang dipantau bot, tanpa restart (berlaku di scan berikutnya):\n"
            "  <code>/pairs</code> → lihat pair sekarang\n"
            "  <code>/pairs all</code> → semua pair\n"
            "  <code>/pairs 15</code> → 15 pair pertama\n"
            "  <code>/pairs BTC,ETH,SOL</code> → hanya pair ini\n"
            "  <code>/pairs add DOGE</code> / <code>/pairs remove DOGE</code>\n\n"
            "/maxtradesperday &lt;jumlah&gt;\n"
            "  Batas jumlah trade yang boleh DIBUKA per hari UTC (0 = tanpa batas).\n"
            "  Yang dihitung hanya order yang TERISI; reset otomatis UTC 00:00,\n"
            "  contoh: <code>/maxtradesperday 5</code>\n\n"
            "/changeliveordemo\n"
            "  Toggle mode LIVE ↔ DEMO (testnet/mainnet)\n"
            "  ⚠️ LIVE = uang asli! Bot otomatis cek & kirim IP publik untuk di-whitelist.\n\n"
            "/togglestochgate\n"
            "  Toggle hard gate Stochastic (5,3,3) per posisi. Kalau ON: sinyal\n"
            "  SHORT wajib pair-nya sudah OVERBOUGHT, sinyal LONG wajib sudah\n"
            "  OVERSOLD — kalau belum, sinyal di-skip total (bukan penalti score).\n\n"
            "/backtest [hari] [all|jumlah|PAIR,PAIR]\n"
            "  Backtest strategi di data historis Binance (proses terpisah, log\n"
            "  TERPISAH dari trade_log). Contoh: <code>/backtest 7 15</code>,\n"
            "  <code>/backtest 14 all</code>. Juga: <code>/backtest status|stop|last</code>.\n"
            "  Butuh file backtest.py di samping main.py.\n\n"
            "/getcode\n"
            "  Kirim file main.py yang sedang jalan di Railway ke chat ini\n"
            "  (cadangan kalau GitHub tidak bisa dibuka).\n\n"
            "/resetpnl\n"
            "  Reset PnL kumulatif mode aktif (LIVE atau DEMO)\n\n"
            "/resetstate confirm\n"
            "  Hapus bot_state.json — semua setting (TP1_PARTIAL, bot_paused,\n"
            "  custom threshold, cooldown, dll) kembali ke default kode.\n"
            "  Ditolak kalau masih ada posisi aktif. Env var Railway (API key/\n"
            "  Telegram) TIDAK terpengaruh — dipakai buat ganti hapus-buat-ulang\n"
            "  project cuma buat reset setting. Perlu restart manual setelahnya.\n\n"
            "/cleanuporders\n"
            "  Cancel manual semua orphan orders (SL/TP tanpa posisi aktif).\n"
            "  Otomatis juga jalan tiap 5 menit di background, ini untuk cek cepat.\n\n"
            "/cleanupduplicate [confirm]\n"
            "  Cancel SL/TP order yang MENUMPUK di posisi yang MASIH aktif\n"
            "  (beda dari /cleanuporders — itu cuma urus symbol tanpa posisi).\n"
            "  <code>/cleanupduplicate</code>         → preview dulu (dry-run)\n"
            "  <code>/cleanupduplicate confirm</code>  → beneran cancel duplikat\n"
            "  Otomatis juga jalan tiap 5 menit di background.\n\n"
            "/exportlog [hari]\n"
            "  Download riwayat trading (CSV) untuk dievaluasi.\n"
            "  <code>/exportlog</code>     → semua history\n"
            "  <code>/exportlog 30</code>  → 30 hari terakhir\n\n"
            "/resetlog confirm\n"
            "  Arsipkan trade_log.json lama, mulai tracking dari nol.\n"
            "  Berguna kalau log kecemar data dari versi bot sebelumnya.\n\n"
            "/clearcooldown <PAIR|all>\n"
            "  Hapus cooldown sinyal yang nyangkut (persist di Volume, bisa\n"
            "  bertahan lintas redeploy). Pakai kalau pair ke-skip 'Cooldown\n"
            "  Xj lagi' padahal tidak ada posisi/trade nyata untuk pair itu.\n"
            "  <code>/clearcooldown ATOMUSDT</code> → 1 pair\n"
            "  <code>/clearcooldown all</code>       → semua pair\n\n"
            "/market\n"
            "  Market regime sekarang: % altcoin bullish/bearish + konfirmasi BTC\n"
            "  (BULL_REGIME / BEAR_REGIME / MIXED).\n\n"
            "/scorewr [hari|all]\n"
            "  Breakdown WINRATE per score bucket dari trade log — cek data\n"
            "  asli apakah score gate beneran ngaruh ke WR sebelum diubah.\n"
            "  <code>/scorewr</code>      → 30 hari terakhir\n"
            "  <code>/scorewr 90</code>   → 90 hari terakhir\n"
            "  <code>/scorewr all</code>  → semua data\n\n"
            "/scorebd [hari|all]\n"
            "  Breakdown WINRATE per KOMPONEN skor (BOS/Liquidity/Zone/Volume/\n"
            "  dst) — beda dari /scorewr yang per TOTAL score. Cek komponen\n"
            "  mana yang beneran predictive vs cuma noise.\n"
            "  <code>/scorebd</code>      → 30 hari terakhir\n"
            "  <code>/scorebd all</code>  → semua data\n\n"
        )
    else:
        pass  # Abaikan perintah tidak dikenal


_command_lock = threading.Lock()   # cegah dua command diproses bersamaan


def telegram_polling_loop():
    """
    Background thread yang terus polling update Telegram.
    Hanya memproses message dari TELEGRAM_CHAT_ID (owner).

    Fix double-fire:
    - deleteWebhook saat startup — webhook + polling tidak bisa bersamaan (salah satu menyebabkan dobel)
    - Hanya proses update.message (bukan edited_message) — edit pesan tidak trigger command
    - Dedup via _processed_update_ids: set update_id yang sudah diproses, cegah double execute
      jika polling sempat dapat update yang sama dua kali (misal reconnect)
    - Update offset SEGERA setelah dapat update_id, sebelum proses command
    - _command_lock: mutex agar hanya 1 command diproses dalam satu waktu (cegah race condition)
    """
    global _tg_offset
    _processed_update_ids: set = set()   # dedup dalam 1 sesi bot
    print("🤖 Telegram command listener aktif...")

    # ── Hapus webhook aktif jika ada ─────────────────────────────────────────
    # Jika ada webhook aktif, Telegram split delivery → command bisa jalan dua kali.
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook",
            data={"drop_pending_updates": False},
            timeout=10,
        )
        wh_result = r.json()
        if wh_result.get("result"):
            print("✅ Webhook dihapus — polling mode aktif")
        else:
            print(f"ℹ️ deleteWebhook: {wh_result.get('description', 'OK')}")
    except Exception as _we:
        print(f"⚠️ deleteWebhook error (lanjut): {_we}")

    # ── Flush pending updates lama sebelum mulai proses command ─────────────
    # Saat bot restart, Telegram masih punya antrian update lama (command yang
    # dikirim sebelum bot mati). Kalau langsung diproses → bot bisa tiba-tiba
    # pause/resume/start sendiri.
    #
    # FIX: Hanya flush update yang timestamp-nya LEBIH LAMA dari waktu bot start.
    # Command yang dikirim SETELAH bot start (dalam 60 detik terakhir) tetap diproses.
    # Ini mencegah /start yang baru dikirim ikut ke-flush.
    _bot_start_unix = int(time.time())   # timestamp saat polling mulai
    try:
        _flush = tg_get_updates(0)
        flushed_count = 0
        if _flush:
            for u in _flush:
                # FIX: update bisa berupa message ATAU callback_query (tombol) —
                # field tanggalnya beda lokasi (callback_query.message.date, bukan
                # message.date). Sebelumnya hanya cek u["message"]["date"], jadi
                # untuk update callback_query, msg_date selalu ke-default 0 —
                # SELALU dianggap "update lama" dan ikut di-flush, walau
                # tombolnya baru saja di-tap user. Ini root cause tombol menu
                # tidak berefek sama sekali kalau di-tap dalam ~30 detik pertama
                # setelah bot start/redeploy.
                _msg_obj = u.get("message") or u.get("callback_query", {}).get("message", {})
                msg_date = _msg_obj.get("date", 0)
                # Flush update yang dikirim SEBELUM bot start (bukan dalam 30 detik terakhir)
                if msg_date < _bot_start_unix - 30:
                    _processed_update_ids.add(u["update_id"])
                    flushed_count += 1
                    # Update offset hanya untuk update yang di-flush
                    if u["update_id"] + 1 > _tg_offset:
                        _tg_offset = u["update_id"] + 1
            if flushed_count:
                print(f"🧹 Flushed {flushed_count} pending Telegram updates (offset now {_tg_offset})")
            kept = len(_flush) - flushed_count
            if kept:
                print(f"📬 {kept} update baru dipertahankan untuk diproses (dikirim < 30 detik lalu)")
    except Exception as _fe:
        print(f"⚠️ Flush pending updates error: {_fe}")

    while True:
        try:
            updates = tg_get_updates(_tg_offset)
            for update in updates:
                uid = update["update_id"]
                # Selalu update offset lebih dulu — cegah re-fetch update yang sama
                if uid + 1 > _tg_offset:
                    _tg_offset = uid + 1

                # Skip jika update_id ini sudah pernah diproses (dedup)
                if uid in _processed_update_ids:
                    continue
                _processed_update_ids.add(uid)
                # Jaga ukuran set agar tidak membengkak (simpan 500 terakhir, hapus 100 terlama)
                if len(_processed_update_ids) > 500:
                    oldest = sorted(_processed_update_ids)[:100]
                    for old_id in oldest:
                        _processed_update_ids.discard(old_id)

                # Hanya proses message biasa — ABAIKAN edited_message
                # edited_message terjadi saat user edit pesan lama → tidak boleh re-trigger command
                msg = update.get("message")
                callback = update.get("callback_query")

                # ── Handle inline keyboard button tap (callback_query) ────────
                if callback:
                    cb_id      = callback.get("id", "")
                    cb_chat_id = str(callback.get("message", {}).get("chat", {}).get("id", ""))
                    cb_data    = callback.get("data", "")
                    if cb_chat_id == TELEGRAM_CHAT_ID and cb_data.startswith("/"):
                        print(f"📱 Button tap: {cb_data} (update_id={uid})")
                        # Jawab callback dulu agar tombol tidak loading
                        answer_callback_query(cb_id, "⚙️ Memproses...")
                        # FIX: handle_command() dijalankan di thread terpisah, BUKAN
                        # inline di polling loop ini. Sebelumnya `with _command_lock:`
                        # di sini BLOCKING — kalau command sebelumnya masih pegang
                        # lock (mis. _close_positions_filtered() yang query Binance
                        # dan kena retry rate-limit 418/429, bisa nunggu sampai ~225
                        # detik), baris ini ikut menunggu, sehingga SATU-SATUNYA
                        # thread yang polling Telegram ini freeze total — tidak ada
                        # update BARU yang di-fetch sama sekali selama itu. Efeknya:
                        # tombol lain yang di-tap selama window itu terlihat "diam
                        # total", padahal sebenarnya request-nya belum sempat
                        # di-fetch dari Telegram. Menjalankan di thread terpisah
                        # membuat polling loop tetap jalan mengambil update baru,
                        # sementara _command_lock (dipegang di dalam thread ini)
                        # tetap menjamin command tidak dieksekusi bertumpukan.
                        def _run_cb_command(_data=cb_data):
                            try:
                                with _command_lock:
                                    handle_command(_data)
                            except Exception as _e:
                                print(f"⚠️ Callback command error: {_e}")
                        threading.Thread(target=_run_cb_command, daemon=True).start()
                    continue

                if not msg:
                    continue
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id != TELEGRAM_CHAT_ID:
                    continue  # Abaikan pesan dari luar owner
                text = msg.get("text", "")
                if text.startswith("/"):
                    print(f"📨 Command diterima: {text} (update_id={uid})")
                    # FIX: sama seperti callback di atas — dijalankan di thread
                    # terpisah supaya polling loop tidak freeze kalau command ini
                    # butuh waktu lama (retry Binance API, dsb).
                    def _run_text_command(_text=text):
                        try:
                            with _command_lock:   # mutex: cegah dua command jalan bersamaan
                                handle_command(_text)
                        except Exception as _e:
                            print(f"⚠️ Command error: {_e}")
                    threading.Thread(target=_run_text_command, daemon=True).start()
        except Exception as e:
            print(f"⚠️ Polling error: {e}")

        time.sleep(0.5)   # ← FIX: tidak perlu besar, long-poll sudah blocking di Telegram server


# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20 — HTF RANGING ADAPTIVE ENGINE (v5 NEW)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class RangingContext:
    """Hasil evaluasi saat HTF sedang ranging."""
    allowed:                bool          = False
    direction:              Optional[str] = None
    ltf_score:              int           = 0
    reasons:                list          = field(default_factory=list)
    score_breakdown:        dict          = field(default_factory=dict)
    bos_confirmed:          bool          = False
    bos_strength:           str           = "NONE"
    displacement_confirmed: bool          = False
    volume_spike:           bool          = False
    in_ob:                  bool          = False
    in_fvg:                 bool          = False
    session_ok:             bool          = False
    range_position:         str           = "MID"
    ltf_momentum:           str           = "NONE"
    near_range_edge:        bool          = False


def _ranging_atr(df: pd.DataFrame, period: int = 14) -> float:
    trs = []
    for i in range(1, len(df)):
        h  = float(df.iloc[i]["high"])
        lo = float(df.iloc[i]["low"])
        pc = float(df.iloc[i - 1]["close"])
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    return sum(trs[-period:]) / min(period, len(trs)) if trs else 0.0


def _is_range_wide_enough(df_htf: pd.DataFrame) -> bool:
    """Range HTF harus minimal RANGE_ATR_FACTOR × ATR agar layak di-trade."""
    atr        = _ranging_atr(df_htf)
    lookback   = min(50, len(df_htf))
    recent     = df_htf.iloc[-lookback:]
    range_span = float(recent["high"].max()) - float(recent["low"].min())
    return range_span >= atr * RANGE_ATR_FACTOR


def _assess_range_position(df_htf: pd.DataFrame) -> tuple:
    """
    Deteksi posisi harga di dalam range HTF.
    Returns: (position: str, pct_from_bottom: float)
    """
    lookback   = min(50, len(df_htf))
    recent     = df_htf.iloc[-lookback:]
    range_high = float(recent["high"].max())
    range_low  = float(recent["low"].min())
    price      = float(df_htf["close"].iloc[-1])
    full_range = range_high - range_low

    if full_range < 1e-9:
        return "MID", 0.5

    pct = (price - range_low) / full_range

    if pct >= (1 - RANGE_EDGE_BUFFER_PCT):  return "NEAR_HIGH", pct
    if pct <= RANGE_EDGE_BUFFER_PCT:        return "NEAR_LOW",  pct
    return "MID", pct


def _detect_ltf_bos(df: pd.DataFrame, direction: str, window: int = 5) -> tuple:
    """
    Deteksi BOS pada LTF entry TF.
    Returns: (confirmed: bool, strength: str, body_pct: float)
    """
    if len(df) < window * 2 + 3:
        return False, "NONE", 0.0

    lookback = df.iloc[-(window * 2 + 2):-2]
    recent   = df.iloc[-3:]

    if direction == "BULLISH":
        pivot = float(lookback["high"].max())
        broke = any(float(c["close"]) > pivot for _, c in recent.iterrows())
    else:
        pivot = float(lookback["low"].min())
        broke = any(float(c["close"]) < pivot for _, c in recent.iterrows())

    if not broke:
        return False, "NONE", 0.0

    bos_candle   = df.iloc[-2]
    o, h, lo, cl = (float(bos_candle[x]) for x in ["open", "high", "low", "close"])
    candle_range = h - lo
    body         = abs(cl - o)
    body_pct     = (body / candle_range * 100) if candle_range > 0 else 0.0

    is_directional = (cl > o) if direction == "BULLISH" else (cl < o)
    if not is_directional:
        return True, "WEAK", body_pct

    strength = "STRONG" if body_pct / 100 >= STRONG_BOS_BODY_PCT else "WEAK"
    return True, strength, body_pct


def _detect_ltf_momentum(df: pd.DataFrame, n: int = LTF_CONSECUTIVE_MIN) -> str:
    """N candle berturut-turut arah sama → momentum terkonfirmasi."""
    if len(df) < n + 1:
        return "NONE"
    recent = df.iloc[-n:]
    bull   = all(float(c["close"]) > float(c["open"]) for _, c in recent.iterrows())
    bear   = all(float(c["close"]) < float(c["open"]) for _, c in recent.iterrows())
    if bull: return "BULLISH"
    if bear: return "BEARISH"
    return "NONE"


def _detect_ranging_displacement(df: pd.DataFrame, direction: str) -> bool:
    """
    Displacement candle khusus mode ranging:
    - Body ≥ DISPLACEMENT_BODY_RATIO_MIN × range
    - Body ≥ 1.5× rata-rata body 10 candle terakhir
    - Arah sesuai direction
    """
    for candle in [df.iloc[-1], df.iloc[-2]]:
        o, h, lo, cl = (float(candle[x]) for x in ["open", "high", "low", "close"])
        candle_range = h - lo
        if candle_range < 1e-9:
            continue

        body       = abs(cl - o)
        body_ratio = body / candle_range

        directional = (cl > o) if direction == "BULLISH" else (cl < o)
        if not directional or body_ratio < DISPLACEMENT_BODY_RATIO_MIN:
            continue

        recent_bodies = [
            abs(float(df.iloc[i]["close"]) - float(df.iloc[i]["open"]))
            for i in range(-12, -2)
            if len(df) >= 12
        ]
        avg_body = sum(recent_bodies) / len(recent_bodies) if recent_bodies else body
        if body >= avg_body * 1.5:
            return True

    return False


def evaluate_htf_ranging(
    df_htf:    pd.DataFrame,
    df_entry:  pd.DataFrame,
    htf_event: Optional[str],
    pair:      str,
    label:     str,
) -> RangingContext:
    """
    Evaluasi apakah trade layak dilakukan saat HTF ranging.

    Menggantikan logika hard-skip:
        if htf_bias == "RANGING": return

    Dengan sistem scoring LTF 10-tahap. Hanya allow trade jika:
    - Range HTF cukup lebar (bukan tight chop)
    - Harga di tepi range atau ada CHoCH di mid
    - Ada LTF BOS terkonfirmasi
    - Ada minimal 1 confluence (displacement / OB / FVG / volume spike)
    - LTF composite score ≥ LTF_RANGING_SCORE_GATE (40)  # UPDATED
    """
    ctx = RangingContext()
    bd  = {}

    # ── 0. Gate: range HTF harus cukup lebar ─────────────────────────────────
    if not _is_range_wide_enough(df_htf):
        ctx.reasons.append("HTF range terlalu sempit — pure chop ❌")
        return ctx

    # ── 1. Range position ─────────────────────────────────────────────────────
    range_pos, pct_from_bot = _assess_range_position(df_htf)
    ctx.range_position = range_pos

    if range_pos == "NEAR_LOW":
        range_bias = "BULLISH"
        bd["range_position"] = 20
        ctx.reasons.append(f"Harga dekat LOW range ({pct_from_bot*100:.0f}% dari bawah) → Bullish bias ✅")
    elif range_pos == "NEAR_HIGH":
        range_bias = "BEARISH"
        bd["range_position"] = 20
        ctx.reasons.append(f"Harga dekat HIGH range ({pct_from_bot*100:.0f}% dari bawah) → Bearish bias ✅")
    else:
        # Mid-range: hanya izin kalau ada CHoCH, atau lanjut dengan score kecil  # UPDATED
        if htf_event == "CHoCH":
            range_bias = None  # resolusi dari LTF momentum
            bd["range_position"] = 5
            ctx.reasons.append("CHoCH di mid-range → butuh konfirmasi LTF ⚠️")
        else:
            # UPDATED: tidak langsung return, lanjut dengan penalty score kecil
            range_bias = None
            bd["range_position"] = -5
            ctx.reasons.append("⚠️ Harga di mid-range tanpa CHoCH — chop risk, penalti score")

    # ── 2. LTF BOS ────────────────────────────────────────────────────────────
    test_dirs = [range_bias] if range_bias else ["BULLISH", "BEARISH"]
    bos_confirmed, bos_strength, bos_body_pct = False, "NONE", 0.0
    resolved_direction = None

    for d in test_dirs:
        confirmed, strength, body_pct = _detect_ltf_bos(df_entry, d)
        if confirmed and strength in ("STRONG", "WEAK"):
            bos_confirmed, bos_strength, bos_body_pct = confirmed, strength, body_pct
            resolved_direction = d
            break

    ctx.bos_confirmed = bos_confirmed
    ctx.bos_strength  = bos_strength

    if not bos_confirmed:
        # UPDATED: tidak hard return, beri penalty dan lanjut
        bd["ltf_bos"] = -10
        ctx.reasons.append("⚠️ Tidak ada LTF BOS — penalti score, lanjut proses")
    else:
        bos_pts = 25 if bos_strength == "STRONG" else 12
        bd["ltf_bos"] = bos_pts
        ctx.reasons.append(
            f"LTF BOS {resolved_direction} ({bos_strength}) | Body: {bos_body_pct:.1f}% "
            f"{'✅' if bos_strength == 'STRONG' else '⚠️'}"
        )

    # ── 3. Consecutive momentum ───────────────────────────────────────────────
    # UPDATED: jika BOS tidak terkonfirmasi, gunakan range_bias atau deteksi dari candle
    if resolved_direction is None:
        resolved_direction = range_bias or ("BULLISH" if float(df_entry["close"].iloc[-1]) > float(df_entry["open"].iloc[-1]) else "BEARISH")

    momentum = _detect_ltf_momentum(df_entry)
    ctx.ltf_momentum = momentum

    if momentum == resolved_direction:
        bd["momentum"] = 10
        ctx.reasons.append(f"✅ {LTF_CONSECUTIVE_MIN} candle momentum {momentum} terkonfirmasi")
    elif momentum == "NONE":
        bd["momentum"] = 0
        ctx.reasons.append("⚠️ Momentum LTF campuran (tidak konsisten)")
    else:
        bd["momentum"] = -5  # UPDATED: dikurangi dari -15 → -5
        ctx.reasons.append(f"⚠️ Momentum LTF berlawanan dengan BOS — penalti ringan")

    # ── 4. Displacement candle ────────────────────────────────────────────────
    has_disp = _detect_ranging_displacement(df_entry, resolved_direction)
    ctx.displacement_confirmed = has_disp
    bd["displacement"] = 15 if has_disp else 0
    ctx.reasons.append(
        "✅ Displacement candle terkonfirmasi" if has_disp else "⚠️ Tidak ada displacement candle"
    )

    # ── 5. Volume spike ───────────────────────────────────────────────────────
    vol_rat       = volume_ratio(df_entry)
    has_vol_spike = vol_rat >= VOLUME_SPIKE_MULTIPLIER
    ctx.volume_spike = has_vol_spike

    if has_vol_spike:
        bd["volume_spike"] = 10
        ctx.reasons.append(f"✅ Volume spike: {vol_rat}× avg (≥{VOLUME_SPIKE_MULTIPLIER}×)")
    elif vol_rat >= 1.5:
        bd["volume_spike"] = 5
        ctx.reasons.append(f"⚡ Volume elevated: {vol_rat}× avg")
    else:
        bd["volume_spike"] = 0
        ctx.reasons.append(f"⚠️ Volume normal: {vol_rat}× avg")

    # ── 6. OB / FVG confluence ────────────────────────────────────────────────
    price  = float(df_entry["close"].iloc[-1])
    ob_list = find_order_blocks(df_entry, resolved_direction)
    fvg    = find_fvg(df_entry, resolved_direction)
    in_ob, _ = price_in_ob(price, ob_list)
    in_fvg   = price_in_fvg(price, fvg)

    ctx.in_ob  = in_ob
    ctx.in_fvg = in_fvg

    if in_ob and in_fvg:
        bd["ob_fvg"] = 15
        ctx.reasons.append("✅ OB + FVG confluence")
    elif in_ob:
        bd["ob_fvg"] = 10
        ctx.reasons.append("✅ Harga di dalam Order Block")
    elif in_fvg:
        bd["ob_fvg"] = 8
        ctx.reasons.append("✅ Harga di dalam Fair Value Gap")
    else:
        bd["ob_fvg"] = 0
        ctx.reasons.append("⚠️ Tidak ada OB/FVG (pure momentum play)")

    # ── 7. Session ────────────────────────────────────────────────────────────
    session    = get_session()
    sess_bonus = SESSION_BONUS_MAP.get(session, 0)
    ctx.session_ok = session in ("London", "New York")
    bd["session"] = sess_bonus
    ctx.reasons.append(
        f"{'✅' if ctx.session_ok else '⚠️'} Session: {session} (+{sess_bonus} pts)"
    )

    # ── 8. Edge penalty ───────────────────────────────────────────────────────
    edge_penalty = 0
    if range_pos == "NEAR_HIGH" and resolved_direction == "BULLISH":
        edge_penalty = -20
        ctx.near_range_edge = True
        ctx.reasons.append("❌ BULLISH dekat HIGH range — fade risk → penalti besar")
    elif range_pos == "NEAR_LOW" and resolved_direction == "BEARISH":
        edge_penalty = -20
        ctx.near_range_edge = True
        ctx.reasons.append("❌ BEARISH dekat LOW range — fade risk → penalti besar")
    bd["edge_penalty"] = edge_penalty

    # ── 9. Hitung total score ─────────────────────────────────────────────────
    total = max(0, min(100, sum(bd.values())))
    ctx.ltf_score       = total
    ctx.score_breakdown = bd
    ctx.direction       = resolved_direction

    # ── 10. Confluence check (penalty, bukan hard skip) ───────────────────────  # UPDATED
    has_confluence = has_disp or in_ob or in_fvg or has_vol_spike or bos_strength == "STRONG"  # UPDATED
    if not has_confluence:
        # UPDATED: tidak return, tambah penalty kecil lalu recalc
        bd["confluence_penalty"] = -5  # UPDATED: penalty ringan -5
        ctx.reasons.append("⚠️ Tidak ada confluence kuat — penalti score kecil")
        total = max(0, min(100, sum(bd.values())))  # recalc setelah penalty
        ctx.ltf_score       = total
        ctx.score_breakdown = bd
    else:
        bd["confluence_penalty"] = 0

    # ── 11. Gate final score ──────────────────────────────────────────────────  # UPDATED
    if total < LTF_RANGING_SCORE_GATE:
        # UPDATED: tidak return langsung, log saja lalu biarkan ctx.allowed = False
        ctx.reasons.append(
            f"⚠️ LTF score {total} < gate {LTF_RANGING_SCORE_GATE} → tidak disetujui"
        )
        # ctx.allowed tetap False (default), return tanpa sinyal
        return ctx

    ctx.allowed = True
    ctx.reasons.append(f"✅ HTF RANGING trade DISETUJUI | LTF score: {total}/100")
    return ctx


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20b — HIGHER TIMEFRAME FILTER (DAILY)
# ═══════════════════════════════════════════════════════════════════════════
# DIBALIK per permintaan user: HTF bias utama sekarang di TF 4h (STEP 1 di
# analyze_pair, lihat SECTION 21), bukan lagi 1h. H4 sudah jadi penentu arah
# utama, jadi cek "H4 vs H4" di sini jadi tidak berguna — dihapus. Yang
# tersisa cuma Daily sebagai filter trend paling besar, DI ATAS bias 4h:
# kalau Daily punya bias JELAS dan BERLAWANAN dengan bias 4h, sinyal di-skip
# (4h dianggap cuma pullback dari trend Daily yang belum berubah). Kalau
# Daily RANGING/UNKNOWN, tidak memblokir apapun — trend besar belum jelas,
# serahkan sepenuhnya ke bias 4h + struktur entry TF 1h.

_daily_bias_cache: dict = {}
DAILY_BIAS_CACHE_TTL = 900   # 15 menit — Daily tidak berubah signifikan tiap scan

# ── Daily Breakout-Retest Parameters ─────────────────────────────────────────
# Level Daily S/R di sini SENGAJA bukan dari order block/zona — murni level
# breakout: swing high/low Daily lama yang sudah ditembus closing (bukan cuma
# wick), lalu di-retest (harga balik ke level itu) dan REJECT balik ke arah
# breakout (candle close balik nembus level retest, bukan cuma nyentuh/wick).
# Contoh (chart SOL user): resistance lama ditembus close di atasnya, retest
# turun ke level situ, lalu candle close naik lagi dari situ → BULLISH.
DAILY_BREAKOUT_LOOKBACK = 60   # jumlah candle Daily ke belakang untuk cari level breakout
DAILY_RETEST_TOLERANCE_PCT = 1.0  # % toleransi jarak candle low/high dari level saat dianggap "menyentuh" retest


def get_daily_breakout_retest_bias(pair: str) -> str:
    """
    Cek apakah pair sudah breakout Daily dari level S/R lama (swing high/low),
    lalu retest level itu dan reject balik searah breakout — dipakai sebagai
    filter arah HTF tambahan (sejajar BTC bias), BUKAN hard gate: kalau belum
    ada breakout+retest valid, return 'RANGING' dan tidak memblokir apapun
    (trade tetap boleh jalan normal berdasarkan bias 4h seperti biasa).

    Return: 'BULLISH' | 'BEARISH' | 'RANGING' | 'UNKNOWN' (fetch gagal)

    Logic:
      1. Cari semua swing high/low Daily (DAILY_BREAKOUT_LOOKBACK candle terakhir).
      2. Untuk tiap level, cari candle SETELAHNYA yang closing sudah tembus
         (close > level untuk resistance, close < level untuk support) —
         itu candle breakout.
      3. Setelah breakout, cari candle SETELAH breakout yang low/high-nya
         balik menyentuh level tsb (dalam DAILY_RETEST_TOLERANCE_PCT) — itu
         candle retest.
      4. Candle SETELAH retest wajib closing sudah reject balik ke arah
         breakout (close di atas level lagi untuk kasus resistance-jadi-
         support, atau close di bawah level lagi untuk support-jadi-
         resistance) — kalau belum ada reaksi ini, belum dianggap valid.
      5. Ambil breakout+retest paling baru (paling relevan) kalau ada lebih
         dari satu match.
    """
    now = time.time()
    cache_key = f"{pair}_breakout_retest"
    cached = _daily_bias_cache.get(cache_key)
    if cached and (now - cached["ts"]) < DAILY_BIAS_CACHE_TTL:
        return cached["daily"]

    result = "UNKNOWN"
    try:
        df_daily = fetch_ohlcv_realdata(pair, "1d", limit=DAILY_BREAKOUT_LOOKBACK + 20)
        if df_daily is not None and len(df_daily) >= 30:
            highs, lows = find_swings(df_daily)
            closes = df_daily["close"].values
            lows_arr  = df_daily["low"].values
            highs_arr = df_daily["high"].values
            n = len(df_daily)

            best_match = None  # (candle_idx, direction) — ambil paling baru

            # Resistance broken → jadi support → retest → reject naik (BULLISH)
            for sh_idx, sh_level in highs:
                for i in range(sh_idx + 1, n):
                    if closes[i] > sh_level:
                        breakout_idx = i
                        tol = sh_level * (DAILY_RETEST_TOLERANCE_PCT / 100.0)
                        for j in range(breakout_idx + 1, n - 1):
                            if lows_arr[j] <= sh_level + tol:
                                # retest ketemu di candle j, cek candle j+1 reject naik
                                if closes[j + 1] > sh_level:
                                    if best_match is None or j + 1 > best_match[0]:
                                        best_match = (j + 1, "BULLISH")
                                break  # cuma retest pertama setelah breakout ini yang relevan
                        break  # cuma breakout pertama dari level ini yang relevan

            # Support broken → jadi resistance → retest → reject turun (BEARISH)
            for sl_idx, sl_level in lows:
                for i in range(sl_idx + 1, n):
                    if closes[i] < sl_level:
                        breakout_idx = i
                        tol = sl_level * (DAILY_RETEST_TOLERANCE_PCT / 100.0)
                        for j in range(breakout_idx + 1, n - 1):
                            if highs_arr[j] >= sl_level - tol:
                                if closes[j + 1] < sl_level:
                                    if best_match is None or j + 1 > best_match[0]:
                                        best_match = (j + 1, "BEARISH")
                                break
                        break

            if best_match is not None:
                result = best_match[1]
            else:
                result = "RANGING"
    except Exception as e:
        print(f"  ⚠️  get_daily_breakout_retest_bias({pair}) error: {e}")

    _daily_bias_cache[cache_key] = {"daily": result, "ts": now}
    return result


# ── Daily Resistance Exhaustion SHORT — parameter ────────────────────────────
# Kasus terpisah dari breakout-retest di atas: bukan continuation habis
# breakout, tapi REVERSAL — harga masih nempel/dekat resistance Daily lama
# (belum breakout), tapi buyer sudah "panas" (RSI Daily & H4 sama-sama
# overbought) + ada candle rejection di level itu. Kalau kondisi ini
# kepenuhi, bot diizinkan cari entry SHORT lewat H4 (arah) → H1 (entry)
# meski gate Daily-alignment biasa (STEP 1b/1c) sebenarnya menolak arah
# BEARISH — TAPI H4 tetap wajib menunjukkan struktur turun sendiri (BOS
# turun di entry_tf), exhaustion ini cuma membuka izin, bukan memaksa arah.
EXHAUSTION_RESISTANCE_TOLERANCE_PCT = 1.5   # % jarak harga sekarang dianggap "nempel" resistance Daily
EXHAUSTION_RSI_DAILY_OVERBOUGHT = 70
EXHAUSTION_RSI_H4_OVERBOUGHT    = 70
_exhaustion_cache: dict = {}
EXHAUSTION_CACHE_TTL = 900   # 15 menit, sama seperti cache Daily lain


def check_daily_resistance_exhaustion_short(pair: str) -> bool:
    """
    True kalau: harga Daily lagi nempel di resistance Daily lama (dalam
    EXHAUSTION_RESISTANCE_TOLERANCE_PCT) DAN RSI Daily overbought DAN RSI H4
    overbought DAN ada minimal 1 candle rejection di resistance itu (upper
    wick panjang / candle close balik turun setelah sempat tembus).
    Return False kalau data tidak cukup/fetch gagal — tidak membuka izin
    apapun kalau ragu.
    """
    now = time.time()
    cached = _exhaustion_cache.get(pair)
    if cached and (now - cached["ts"]) < EXHAUSTION_CACHE_TTL:
        return cached["result"]

    result = False
    try:
        df_daily = fetch_ohlcv_realdata(pair, "1d", limit=100)
        df_h4    = fetch_ohlcv_realdata(pair, "4h", limit=100)
        if df_daily is not None and len(df_daily) >= 30 and df_h4 is not None and len(df_h4) >= 20:
            highs, _ = find_swings(df_daily)
            current_price = float(df_daily["close"].iloc[-1])

            # Resistance Daily terdekat DI ATAS/DEKAT harga sekarang (belum breakout jauh)
            candidate_levels = [lvl for _, lvl in highs if lvl >= current_price * (1 - EXHAUSTION_RESISTANCE_TOLERANCE_PCT / 100.0)]
            if candidate_levels:
                resistance = min(candidate_levels)  # level terdekat dari atas
                tol = resistance * (EXHAUSTION_RESISTANCE_TOLERANCE_PCT / 100.0)
                near_resistance = abs(current_price - resistance) <= tol or (
                    float(df_daily["high"].iloc[-1]) >= resistance - tol
                )

                rsi_daily = calculate_rsi(df_daily)
                rsi_h4    = calculate_rsi(df_h4)

                # Candle rejection: minimal 1 dari 3 candle Daily terakhir close
                # balik di bawah resistance meski high/wick sempat menyentuh/tembus,
                # ATAU upper wick panjang (wick >= 40% dari range candle) di dekat level.
                rejection = False
                for idx in range(-3, 0):
                    c = df_daily.iloc[idx]
                    o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
                    rng = h - l
                    if rng <= 0:
                        continue
                    upper_wick = h - max(o, cl)
                    touched = h >= resistance - tol
                    closed_back_below = cl < resistance
                    long_upper_wick = (upper_wick / rng) >= 0.4
                    if touched and closed_back_below and long_upper_wick:
                        rejection = True
                        break

                result = (
                    near_resistance
                    and rsi_daily >= EXHAUSTION_RSI_DAILY_OVERBOUGHT
                    and rsi_h4 >= EXHAUSTION_RSI_H4_OVERBOUGHT
                    and rejection
                )
    except Exception as e:
        print(f"  ⚠️  check_daily_resistance_exhaustion_short({pair}) error: {e}")
        result = False

    _exhaustion_cache[pair] = {"result": result, "ts": now}
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 20c — BTC 4H RANGING → BLOCK SHORT ALTCOIN (GATE BARU)
# ═══════════════════════════════════════════════════════════════════════════
# Permintaan: BTC di TF 4 jam jadi acuan arah tambahan.
#   • BTC 4H BEARISH → siap ambil SHORT (tidak diblok gate ini)
#   • BTC 4H RANGING → SHORT altcoin DIBLOK, LONG tetap boleh jalan
#   • BTC 4H BULLISH → SHORT altcoin DIBLOK (lawan arus market)
# Ini gate TAMBAHAN, terpisah dari macro block BTC_BIAS_TF=1h yang sudah
# ada di STEP 4a existing di bawah — tidak menggantikan, cuma menambah
# syarat baru dari sudut pandang BTC 4H. Exempt BTC/ETH sendiri, sama
# seperti macro block yang lama (tidak relevan dikorelasikan ke dirinya
# sendiri).
# ─────────────────────────────────────────────────────────────────────────
_btc_h4_bias_cache: dict = {"bias": "UNKNOWN", "ts": 0.0}
BTC_H4_BIAS_CACHE_TTL = 900   # 15 menit — cukup untuk candle 4h, hindari fetch berulang


def get_btc_h4_bias() -> str:
    """
    Bias BTC di TF 4 jam, di-cache global (bukan per-pair — BTC cuma satu)
    supaya tidak fetch berulang untuk tiap pair di tiap scan.
    Return: 'BULLISH' / 'BEARISH' / 'RANGING' / 'UNKNOWN' (fetch gagal —
    diperlakukan sama seperti RANGING oleh pemanggil, yaitu tidak
    memblokir SHORT, karena kita tidak mau bot berhenti total kalau
    fetch BTC 4h kebetulan gagal sesaat).
    """
    now = time.time()
    if (now - _btc_h4_bias_cache["ts"]) < BTC_H4_BIAS_CACHE_TTL:
        return _btc_h4_bias_cache["bias"]

    bias = "UNKNOWN"
    try:
        df_btc_h4 = fetch_ohlcv_realdata("BTC/USDT", "4h", limit=150)
        if df_btc_h4 is not None and len(df_btc_h4) >= 30:
            bias, _, _, _ = detect_structure(df_btc_h4)
    except Exception as e:
        print(f"  ⚠️  get_btc_h4_bias() fetch error: {e}")

    _btc_h4_bias_cache["bias"] = bias
    _btc_h4_bias_cache["ts"]   = now
    return bias


def get_daily_h4_bias(pair: str) -> tuple:
    """
    Fetch bias Daily (1d) untuk sebuah pair, dengan cache per-pair supaya
    tidak membebani rate limit (dipanggil untuk setiap pair di setiap scan).
    Nama fungsi dipertahankan (get_daily_h4_bias, bukan get_daily_bias) demi
    kompatibilitas — caller di STEP 1b (SECTION 21) sudah tidak butuh nilai
    H4 lagi karena H4 sudah jadi HTF utama di STEP 1, jadi elemen kedua
    dikembalikan "N/A" sebagai penanda tidak lagi dihitung di sini (bukan
    fetch gagal — beda dari 'UNKNOWN').
    Return: (daily_bias, "N/A") — daily_bias salah satu dari 'BULLISH' /
    'BEARISH' / 'RANGING' / 'UNKNOWN' (kalau fetch gagal — UNKNOWN tidak
    memblokir apapun, diperlakukan sama seperti RANGING oleh pemanggil).
    """
    now = time.time()
    cached = _daily_bias_cache.get(pair)
    if cached and (now - cached["ts"]) < DAILY_BIAS_CACHE_TTL:
        return cached["daily"], "N/A"

    daily_bias = "UNKNOWN"
    try:
        df_daily = fetch_ohlcv_realdata(pair, "1d", limit=100)
        if df_daily is not None and len(df_daily) >= 30:
            daily_bias, _, _, _ = detect_structure(df_daily)
    except Exception as e:
        print(f"  ⚠️  get_daily_h4_bias({pair}) Daily fetch error: {e}")

    _daily_bias_cache[pair] = {"daily": daily_bias, "ts": now}
    return daily_bias, "N/A"


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 21 — PAIR ANALYSIS v5 (HTF RANGING ADAPTIVE)
# ═══════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════
# EDGE FILTER (v77) — hasil analisis 135 trade backtest (3 periode)
#   • LONG: -96.5 USDT, rugi di 3 dari 3 periode (WR 27%)  → dimatikan dulu
#   • ATR entry-TF > 0.6%: rugi di periode lama DAN di 3 periode tes baru (OOS: -128.7 vs +133.8 untuk ATR<=0.6) → diblok
# Atur/matikan lewat BOT_OVERRIDES di backtest.py, contoh:
#   {"EDGE_FILTER_ON": False}                    → bandingkan tanpa filter
#   {"EDGE_ALLOWED_DIRS": {"LONG", "SHORT"}}     → aktifkan LONG lagi
#   {"EDGE_MAX_ATR_PCT": 0}                      → matikan batas ATR
# ══════════════════════════════════════════════════════════════════════════
EDGE_FILTER_ON    = True
EDGE_ALLOWED_DIRS = {"LONG", "SHORT"}   # v-struct: dua arah (gate lain simetris); {"SHORT"} = hanya short
EDGE_MAX_ATR_PCT  = 0.6         # ATR% di TF entry; 0 = tanpa batas
EDGE_PAIR_BLOCK   = set()       # opsional, mis. {"AVAXUSDT", "LINKUSDT", "DOGEUSDT"}

# Konfirmasi volume (usulan user) — DEFAULT MATI, hanya untuk dites di backtest.
# Syarat saat nyala: volume candle sinyal >= EDGE_VOL_MIN_RATIO x rata-rata 20 candle,
# DAN lebih tinggi dari volume N candle sebelumnya, DAN warna candle searah entry
# (SHORT = candle merah/seller dominan, LONG = candle hijau/buyer dominan).
EDGE_VOL_CONFIRM_ON = False
EDGE_VOL_MIN_RATIO  = 1.5
EDGE_VOL_LOOKBACK   = 5
EDGE_VOL_NEED_COLOR = True

# Market regime (v78): % altcoin bullish/bearish (1h) + konfirmasi bias BTC.
# BEAR_REGIME = mayoritas alt turun & BTC turun; BULL_REGIME = mayoritas alt naik & BTC naik;
# MIXED = tidak searah. Fungsinya sudah ada di bot (compute_market_regime) tapi sebelumnya tidak dipasang.
#   EDGE_REGIME_LOG_ON  = True  → regime dicatat ke log trade (kolom mkt_regime, alt_bull_pct, alt_bear_pct)
#   EDGE_REGIME_GATE_ON = False → kalau True: SHORT diblok saat BULL_REGIME, LONG diblok saat BEAR_REGIME
EDGE_REGIME_LOG_ON  = True
EDGE_REGIME_GATE_ON = False


def analyze_pair(pair, mode, df_htf, df_ref, btc_bias, btcd_trend, session, signal_candidates, market_regime=None):
    """
    ═══════════════════════════════════════════════════════════════
    SMC FLOW — SCORING MURNI (v6)
    ═══════════════════════════════════════════════════════════════
    Step 1 : HTF → tentukan bias arah
    Step 2 : LTF → cari liquidity target (sweep / reaksi)
    Step 3 : BOS / momentum terkonfirmasi
    Step 4 : Entry di pullback (OB / FVG)
    Step 5 : Hitung score akumulatif → kirim jika ≥ threshold

    Tidak ada hard-gate per komponen.
    Setiap komponen memberi poin positif atau negatif.
    Sinyal SELALU ada kandidat; yang dikirim = yang paling kuat.
    ═══════════════════════════════════════════════════════════════
    """
    label    = mode["label"]
    htf_tf   = mode["htf_tf"]
    entry_tf = mode["entry_tf"]
    tier     = mode.get("tier", "FULL")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — HTF BIAS (TF 4 JAM — penentu arah utama)
    # HTF (4h) WAJIB align dengan arah trade. Jika HTF trending jelas (BOS),
    # pakai itu langsung. Jika HTF RANGING (tidak ada bias jelas dari swing
    # HH/HL atau LH/LL), TIDAK langsung di-skip — market kripto (apalagi
    # top-10, volatile) jarang benar-benar tanpa arah; "RANGING" di sini
    # seringkali cuma berarti swing besar belum align padahal LTF (entry TF
    # 1h) sudah punya struktur/momentum sendiri. Fallback ke
    # evaluate_htf_ranging(): trade tetap boleh jalan HANYA jika lolos
    # scoring LTF 10-tahap (BOS entry TF, momentum, displacement, volume,
    # OB/FVG, dst) dengan skor ≥ LTF_RANGING_SCORE_GATE. Kalau skor tidak
    # cukup, baru benar-benar di-skip.
    # ─────────────────────────────────────────────────────────────────────────
    htf_bias, htf_event, _, _ = detect_structure(df_htf)
    ranging_ctx = None

    # LONG di demand TF tinggi (opsional) — setup reversal, menggantikan gate arah HTF
    _dz_long, _dz_zone, _dz_wick = False, None, None
    _dz_why = ""
    if HTF_DEMAND_MODE:
        _dz_res = _htf_demand_long_check(pair, entry_tf)
        if _dz_res is not None:
            _dz_long, _dz_zone, _dz_wick = True, (_dz_res[0], _dz_res[1]), _dz_res[2]

    # SHORT di supply TF tinggi (OB bearish LuxAlgo, opsional) — kembaran demand, juga menggantikan gate arah HTF
    _sz_short, _sz_zone, _sz_wick = False, None, None
    if HTF_SUPPLY_MODE and not _dz_long:
        _sz_res = _htf_supply_short_check(pair, entry_tf)
        if _sz_res is not None:
            _sz_short, _sz_zone, _sz_wick = True, (_sz_res[0], _sz_res[1]), _sz_res[2]
    _zone_rev = _dz_long or _sz_short   # setup reversal di zona HTF (LONG demand / SHORT supply): gate arah HTF dilewati

    if BLOCK_HTF_RANGING and not RESEARCH_MODE and htf_bias == "RANGING" and not _zone_rev:
        print(f"  ⏭  [{label}] {pair} — HTF {htf_tf} RANGING (sideways), BLOCK_HTF_RANGING aktif, skip")
        return

    # Tentukan trade_direction dari HTF (1:1 PORT dari base signal-only main41)
    if htf_bias != "RANGING":
        trade_direction = htf_bias
        htf_aligned     = True
    else:
        # HTF ranging → cari arah dari LTF momentum / range position
        try:
            # 1:1 PARITY FIX: limit disamakan ke 150 seperti signal-only base
            # (signal.py) — limit=250 mengubah hasil evaluate_htf_ranging()
            # (swing detection & momentum) dibanding referensi.
            df_entry_pre = fetch_ohlcv(pair, entry_tf, limit=150)
            time.sleep(0.08)
        except Exception as e:
            print(f"  ❌ [{label}] {pair} — HTF {htf_tf} RANGING, fetch entry TF gagal: {e}, skip")
            return

        ranging_ctx = evaluate_htf_ranging(df_htf, df_entry_pre, htf_event, pair, label)

        if not ranging_ctx.allowed and not _zone_rev and not (RESEARCH_MODE and getattr(ranging_ctx, "direction", None)):
            print(f"  ⏭  [{label}] {pair} — HTF {htf_tf} RANGING, LTF score {ranging_ctx.ltf_score} "
                  f"< gate {LTF_RANGING_SCORE_GATE} (tidak ada bias jelas), skip")
            return

        trade_direction = ranging_ctx.direction
        htf_aligned      = False   # HTF ranging → penalti di score
        dir_str  = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  🟡 [{label}] {pair} {dir_str} — HTF {htf_tf} RANGING tapi LTF score {ranging_ctx.ltf_score}"
              f"/100 ≥ gate {LTF_RANGING_SCORE_GATE} → lanjut (adaptive)")

    # RELAXED tier (LOW_TF): pakai logika asli, skip ranging tanpa CHoCH
    if tier == "RELAXED":
        if htf_bias == "RANGING" and htf_event != "CHoCH" and not _zone_rev:
            print(f"  ⏭  [{label}] {pair} — HTF {htf_tf} ranging tanpa CHoCH, skip")
            return
        if htf_bias == "RANGING":
            last            = df_htf.iloc[-1]
            trade_direction = "BULLISH" if float(last["close"]) > float(last["open"]) else "BEARISH"
            htf_aligned     = False
        else:
            htf_aligned = True

    if _dz_long:
        trade_direction = "BULLISH"
        htf_aligned     = False   # setup reversal: HTF 1h tidak dianggap searah
    elif _sz_short:
        trade_direction = "BEARISH"
        htf_aligned     = False   # setup reversal: HTF 1h tidak dianggap searah

    # ── SIGNAL COOLDOWN CHECK ──────────────────────────────────────────────────
    # Blok sinyal arah yang sama selama SIGNAL_COOLDOWN_HOURS jam setelah sinyal terakhir.
    # Arah BERLAWANAN tetap boleh (LONG cooldown tidak blok SHORT).
    # Cooldown diangkat otomatis jika: SL kena, posisi expired, atau flip arah.
    on_cd, cd_remaining, cd_reason = is_on_cooldown(pair, trade_direction)
    if on_cd:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⏳ [{label}] {pair} {dir_str} — {cd_reason}")
        return

    # ── TREND GATE (2026-11-10) — hanya entry saat HTF trending & searah ─────
    # ADX/DI selalu dihitung (untuk log); gate hanya memblok kalau TREND_GATE_ON.
    try:
        adx_htf, pdi_htf, ndi_htf = calculate_adx_di(df_htf)
    except Exception:
        adx_htf, pdi_htf, ndi_htf = 25.0, 0.0, 0.0
    di_gap_htf = (pdi_htf - ndi_htf) if trade_direction == "BULLISH" else (ndi_htf - pdi_htf)
    if _sz_short and SZ_REQUIRE_HTF_DI and di_gap_htf < SZ_MIN_DI_GAP:
        SZ_STATS["block_di"] += 1
        print(f"  ⛔ [{label}] {pair} SHORT — SUPPLY: tren {htf_tf} melawan SHORT (DI gap {di_gap_htf:.1f} < {SZ_MIN_DI_GAP:g}), skip")
        return
    if TREND_GATE_ON and not RESEARCH_MODE and not _zone_rev:
        _dir_s = "LONG" if trade_direction == "BULLISH" else "SHORT"
        if TREND_GATE_ADX_MIN > 0 and adx_htf < TREND_GATE_ADX_MIN:
            print(f"  ⛔ [{label}] {pair} {_dir_s} — TREND GATE: ADX {htf_tf} {adx_htf:.1f} "
                  f"< {TREND_GATE_ADX_MIN:g} (market choppy/tengah-tengah), skip")
            return
        if TREND_GATE_DI_ALIGN and pdi_htf != ndi_htf and di_gap_htf < 0:
            print(f"  ⛔ [{label}] {pair} {_dir_s} — TREND GATE: DI {htf_tf} berlawanan "
                  f"(+DI {pdi_htf:.1f} / -DI {ndi_htf:.1f}), skip")
            return
        if TREND_GATE_DI_MIN > 0 and di_gap_htf < TREND_GATE_DI_MIN:
            print(f"  ⛔ [{label}] {pair} {_dir_s} — TREND GATE: DI gap {htf_tf} {di_gap_htf:.1f} "
                  f"< {TREND_GATE_DI_MIN:g} (tren searah terlalu tipis), skip")
            return
        if TREND_GATE_ADX_MAX > 0 and adx_htf > TREND_GATE_ADX_MAX:
            print(f"  ⛔ [{label}] {pair} {_dir_s} — TREND GATE: ADX {htf_tf} {adx_htf:.1f} "
                  f"> {TREND_GATE_ADX_MAX:g} (tren extended/telat), skip")
            return

    # ── SLOW TREND GATE (2026-11-10) — searah rezim lambat EMA50/EMA200 HTF ──
    ema_gap_htf = 0.0
    _slow_ok_data = False
    try:
        if len(df_htf) >= 210:
            _cl = df_htf["close"].astype(float)
            _e50  = float(_cl.ewm(span=50,  adjust=False).mean().iloc[-1])
            _e200 = float(_cl.ewm(span=200, adjust=False).mean().iloc[-1])
            if _e200 > 0:
                _gap = (_e50 - _e200) / _e200 * 100
                ema_gap_htf = _gap if trade_direction == "BULLISH" else -_gap
                _slow_ok_data = True
    except Exception:
        ema_gap_htf, _slow_ok_data = 0.0, False
    if SLOW_TREND_GATE_ON and not RESEARCH_MODE and not _zone_rev and _slow_ok_data and ema_gap_htf < 0:
        _dir_s2 = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⛔ [{label}] {pair} {_dir_s2} — SLOW TREND GATE: EMA50 vs EMA200 {htf_tf} "
              f"berlawanan arah trade (gap {ema_gap_htf:+.2f}%), skip")
        return

    # ── HTF STRUCTURE GATE (v76) — alt harus breakout/breakdown atau struktur jelas ──
    _hs_ok, _hs_er = "", ""
    if HTF_STRUCT_GATE_ON or RESEARCH_MODE:
        try:
            _atr_h = calculate_atr(df_htf)
        except Exception:
            _atr_h = None
        _st_ok, _st_why, _st_info = check_htf_structure(df_htf, trade_direction, _atr_h)
        _hs_ok = int(bool(_st_ok))
        _hs_er = round(float(_st_info["er"]), 3) if _st_info.get("er") is not None else ""
        if not _st_ok and not RESEARCH_MODE and not _zone_rev:
            _dir_s3 = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  ⛔ [{label}] {pair} {_dir_s3} — HTF STRUCTURE GATE: {_st_why}, skip")
            return

    # ─────────────────────────────────────────────────────────────────────────
    # Ref TF bonus
    # ─────────────────────────────────────────────────────────────────────────
    ref_bias    = get_ref_bias(df_ref)
    ref_aligned = (ref_bias == trade_direction)

    # ─────────────────────────────────────────────────────────────────────────
    # Fetch entry TF (full 250 candle)
    # 1:1 PARITY FIX: selalu fetch df_entry baru dengan limit=250, sama seperti
    # signal-only base (signal.py). df_entry_pre (150 candle, khusus deteksi
    # arah saat HTF RANGING) TIDAK di-reuse sebagai df_entry — reuse ini
    # sebelumnya membuat STEP 2+ (sweep/BOS/OB/FVG/RR) jalan di atas dataset
    # yang lebih pendek dari referensi saat HTF ranging, berisiko mengubah
    # hasil sinyal. Ekstra 1 API call per pair saat HTF ranging diterima demi
    # paritas output.
    # ─────────────────────────────────────────────────────────────────────────
    try:
        df_entry = fetch_ohlcv(pair, entry_tf, limit=250)
        time.sleep(0.08)
    except Exception as e:
        print(f"  ❌ [{label}] {pair} fetch failed: {e}")
        return

    current_price = float(df_entry["close"].iloc[-1])
    try:
        entry_atr     = calculate_atr(df_entry)
    except Exception:
        entry_atr     = 0.0
    entry_atr_pct = (entry_atr / current_price * 100) if current_price > 0 else 0.0

    # ── EDGE FILTER (v77) — arah, volatilitas, pair ──
    if EDGE_FILTER_ON and not RESEARCH_MODE and not _zone_rev:
        _edge_dir = "LONG" if trade_direction == "BULLISH" else "SHORT"
        if _edge_dir not in EDGE_ALLOWED_DIRS:
            print(f"  ⛔ [{label}] {pair} {_edge_dir} — EDGE FILTER: arah dimatikan, skip")
            return
        if EDGE_MAX_ATR_PCT > 0 and entry_atr_pct > EDGE_MAX_ATR_PCT:
            print(f"  ⛔ [{label}] {pair} {_edge_dir} — EDGE FILTER: ATR {entry_atr_pct:.2f}% "
                  f"> {EDGE_MAX_ATR_PCT:g}%, skip")
            return
        if pair in EDGE_PAIR_BLOCK:
            print(f"  ⛔ [{label}] {pair} {_edge_dir} — EDGE FILTER: pair diblok, skip")
            return
        if EDGE_VOL_CONFIRM_ON:
            try:
                _vv = df_entry["volume"].astype(float)
                _v_now = float(_vv.iloc[-1])
                _v_avg = float(_vv.iloc[-21:-1].mean())
                _v_prev_max = float(_vv.iloc[-1 - EDGE_VOL_LOOKBACK:-1].max())
                _v_ratio = (_v_now / _v_avg) if _v_avg > 0 else 0.0
                _o_now = float(df_entry["open"].iloc[-1])
                _c_now = float(df_entry["close"].iloc[-1])
                _color_ok = (_c_now < _o_now) if trade_direction == "BEARISH" else (_c_now > _o_now)
                _why = None
                if _v_ratio < EDGE_VOL_MIN_RATIO:
                    _why = f"volume {_v_ratio:.2f}x < {EDGE_VOL_MIN_RATIO:g}x"
                elif _v_now <= _v_prev_max:
                    _why = f"volume tidak lebih tinggi dari {EDGE_VOL_LOOKBACK} candle sebelumnya"
                elif EDGE_VOL_NEED_COLOR and not _color_ok:
                    _why = "warna candle tidak searah entry"
                if _why:
                    print(f"  ⛔ [{label}] {pair} {_edge_dir} — EDGE FILTER: {_why}, skip")
                    return
            except Exception as _e:
                print(f"  ⚠️ [{label}] {pair} — cek volume gagal ({_e}), dilewati")

    # ── MARKET REGIME (v78) — catat ke log, gate opsional ──
    _mkt_regime, _alt_bull, _alt_bear = "?", -1.0, -1.0
    if EDGE_REGIME_LOG_ON or EDGE_REGIME_GATE_ON or REGIME_REQUIRE_ALIGNED:
        try:
            _reg = get_market_regime_signalonly(btc_bias)
            _mkt_regime = _reg.get("regime", "MIXED")
            _alt_bull = round(float(_reg.get("bull_pct", 0.0)) * 100, 1)
            _alt_bear = round(float(_reg.get("bear_pct", 0.0)) * 100, 1)
            if EDGE_REGIME_GATE_ON:
                _reg_blk, _reg_why = check_regime_block(trade_direction, _reg, pair)
                if _reg_blk:
                    print(f"  ⛔ [{label}] {pair} — EDGE FILTER: {_reg_why}, skip")
                    return
            if REGIME_REQUIRE_ALIGNED and not _zone_rev and pair not in ALTCOIN_EXEMPTIONS:
                _need_reg = "BULL_REGIME" if trade_direction == "BULLISH" else "BEAR_REGIME"
                if _mkt_regime != _need_reg:
                    _dir_rg = "LONG" if trade_direction == "BULLISH" else "SHORT"
                    print(f"  ⛔ [{label}] {pair} {_dir_rg} — REGIME ALIGN: regime {_mkt_regime}, butuh {_need_reg}, skip")
                    return
        except Exception as _reg_err:
            print(f"  ⚠️ [{label}] {pair} — cek market regime gagal ({_reg_err}), dilewati")
            if REGIME_REQUIRE_ALIGNED and not _zone_rev and pair not in ALTCOIN_EXEMPTIONS:
                print(f"  ⛔ [{label}] {pair} — REGIME ALIGN: regime tidak diketahui, skip")
                return

    if has_active_position(pair, trade_direction):
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  🚫 [{label}] {pair} — Posisi {dir_str} aktif, skip")
        return

    # Volatility gate (pasar harus bergerak)
    vol_ok, vol_reason = check_volatility(df_entry)
    if not vol_ok:
        print(f"  ⛔ [{label}] {pair} — {vol_reason}")
        return

    # ─────────────────────────────────────────────────────────────────────────
    # price/ob_list dihitung di sini (posisi digeser lebih awal dari base
    # signal-only, tapi secara fungsional setara — tidak dipakai apa pun di
    # STEP 2/3 sebelum STEP 4, jadi hasilnya tetap identik dengan signal.py).
    # ─────────────────────────────────────────────────────────────────────────
    price   = float(df_entry["close"].iloc[-1])
    ob_list = find_order_blocks(df_entry, trade_direction)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — LIQUIDITY TARGET (Sweep / reaksi)
    # ─────────────────────────────────────────────────────────────────────────
    # Coba strong sweep dulu, lalu relaxed — hasilnya masuk scoring, bukan gate
    liq_swept, liq_type, liq_level, sweep_strength = detect_liquidity_sweep(
        df_entry, trade_direction, strict=True
    )
    if not liq_swept:
        liq_swept, liq_type, liq_level, sweep_strength = detect_liquidity_sweep(
            df_entry, trade_direction, strict=False
        )
    # Tidak ada sweep → liq_swept=False, score negatif, tapi proses tetap lanjut

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — BOS / MOMENTUM
    # ─────────────────────────────────────────────────────────────────────────
    entry_bias, entry_event, _, _ = detect_structure(df_entry)
    bos_confirmed  = (entry_bias == trade_direction and entry_event == "BOS")
    bos_str_label  = "STRONG"  # default; bisa di-grade lebih lanjut
    if bos_confirmed:
        # grade kekuatan BOS dari candle terakhir
        last_c     = df_entry.iloc[-1]
        o_c, h_c, l_c, cl_c = float(last_c["open"]), float(last_c["high"]), float(last_c["low"]), float(last_c["close"])
        crange     = h_c - l_c
        body_pct   = abs(cl_c - o_c) / crange if crange > 0 else 0
        bos_str_label = "STRONG" if body_pct >= STRONG_BOS_BODY_PCT else "WEAK"
    else:
        bos_str_label = "NONE"

    # ── VALIDASI ENGULFING (anti-fakeout) ─────────────────────────────────────
    eng_ok, eng_reason = True, ""
    eng_tag, entry_tag, eng_note = "OFF", "OFF", ""
    if ENGULF_BREAK_MODE != "OFF":
        eng_ok, eng_reason = detect_engulfing_break(df_entry, trade_direction)
        eng_tag  = "YES" if eng_ok else "NO"
        eng_note = (eng_reason or "")[:70].replace(",", ";")
        if ENGULF_BREAK_MODE == "GATE" and not eng_ok:
            _dir_e = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  ⛔ [{label}] {pair} {_dir_e} — ENGULFING GATE: {eng_reason}, skip")
            return
        if ENGULF_BREAK_MODE == "SCORE" and not eng_ok and bos_str_label == "STRONG":
            bos_str_label = "WEAK"

    # Momentum: N candle berturut-turut arah sama
    ltf_momentum   = _detect_ltf_momentum(df_entry)
    momentum_align = (ltf_momentum == trade_direction)
    momentum_any   = (ltf_momentum != "NONE")

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — ENTRY ZONE (OB / FVG pullback)
    # ─────────────────────────────────────────────────────────────────────────
    in_ob, best_ob = price_in_ob(price, ob_list)
    fvg          = find_fvg(df_entry, trade_direction)
    in_fvg       = price_in_fvg(price, fvg)

    # Zona retest HTF (opsional)
    _rz_touch = False
    _rz_lo = _rz_hi = None
    if HTF_RETEST_ZONE_MODE:
        if HTF_RETEST_ZONE_SRC == 1:
            _z = _lux_retest_zone(df_htf, trade_direction, price)
        elif HTF_RETEST_ZONE_SRC == 2:
            _z = _lux_retest_zone(df_entry, trade_direction, price)
        else:
            _z = _htf_retest_zone(df_htf, trade_direction)
        if _z is not None:
            _rz_lo, _rz_hi = _z
            _cl_ = df_entry.iloc[-1]
            _hh, _ll, _cc = float(_cl_["high"]), float(_cl_["low"]), float(_cl_["close"])
            if trade_direction == "BULLISH":
                _rz_touch = (_ll <= _rz_hi and _hh >= _rz_lo and _cc >= _rz_lo)
            else:
                _rz_touch = (_hh >= _rz_lo and _ll <= _rz_hi and _cc <= _rz_hi)
        if HTF_RETEST_ZONE_MODE == 1:
            in_ob, best_ob, in_fvg = False, None, False
    if _dz_long:
        _rz_touch = True
        _rz_lo, _rz_hi = _dz_zone
        in_ob, best_ob, in_fvg = False, None, False
    elif _sz_short:
        _rz_touch = True
        _rz_lo, _rz_hi = _sz_zone
        in_ob, best_ob, in_fvg = False, None, False

    if HTF_RETEST_ZONE_MODE and not (in_ob or in_fvg or _rz_touch):
        _dir_rz = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⛔ [{label}] {pair} {_dir_rz} — RETEST ZONE: harga tidak di zona (OB/FVG/HTF), skip")
        return

    # EDGE FILTER v70: entry di FVG-saja (tanpa OB) terbukti negatif di backtest.
    if ENTRY_BLOCK_FVG_ONLY and not RESEARCH_MODE and in_fvg and not in_ob and not _rz_touch:
        _dir_s3 = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⛔ [{label}] {pair} {_dir_s3} — EDGE FILTER: entry zona FVG-saja (tanpa OB), skip")
        return

    if ENTRY_REQUIRE_ZONE and not RESEARCH_MODE and not in_ob and not in_fvg and not _rz_touch:
        _dir_z = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⛔ [{label}] {pair} {_dir_z} — ENTRY ZONE GATE: harga tidak di OB/FVG (entry market), skip")
        return

    # ── PULLBACK EMA 20/50 (opsional; default OFF) ───────────────────────────
    if EMA_PULLBACK_MODE == "GATE":
        _pb_ok, _pb_reason = check_ema_pullback(df_entry, trade_direction, entry_atr)
        if not _pb_ok:
            _dir_pb = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  ⛔ [{label}] {pair} {_dir_pb} — EMA PULLBACK GATE: {_pb_reason}, skip")
            return

    # ── KONFIRMASI ENTRY: engulfing searah trade di candle tertutup terakhir ──
    entry_eng_ok, entry_eng_reason = True, ""
    if ENGULF_BREAK_MODE != "OFF" or ENTRY_ENGULF_MODE == "GATE":
        _e_ok, _e_reason = detect_entry_engulfing(df_entry, trade_direction)
        entry_tag = "YES" if _e_ok else "NO"
    if ENTRY_ENGULF_MODE == "GATE":
        entry_eng_ok, entry_eng_reason = detect_entry_engulfing(df_entry, trade_direction)
        if not entry_eng_ok:
            _dir_e2 = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  ⛔ [{label}] {pair} {_dir_e2} — ENTRY ENGULFING GATE: {entry_eng_reason}, skip")
            return

    # ── RETEST FVG: close di sisi searah dari tengah FVG ─────────────────────
    ret_tag, ret_note = "OFF", ""
    if FVG_RETEST_MODE != "OFF":
        _r_ok, _r_reason = detect_fvg_retest(df_entry, trade_direction)
        ret_tag  = "YES" if _r_ok else "NO"
        ret_note = (_r_reason or "")[:70].replace(",", ";")
        if FVG_RETEST_MODE == "GATE" and not _r_ok:
            _dir_r = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  ⛔ [{label}] {pair} {_dir_r} — FVG RETEST GATE: {_r_reason}, skip")
            return

    # ── STOCHASTIC: divergence / pelemahan momentum (candle tertutup) ────────
    stoch_div_tag, stoch_weak_tag, stoch_note = "OFF", "OFF", ""
    if STOCH_CONFIRM_MODE != "OFF":
        try:
            _dc  = df_entry.iloc[:-1]
            _st  = calculate_stochastic(_dc)
            _dv, _dt, _dr = detect_stoch_divergence(_dc, _st, trade_direction)
            _wk, _wr      = detect_stoch_weakening(_st, trade_direction)
            stoch_div_tag  = "YES" if _dv else "NO"
            stoch_weak_tag = "YES" if _wk else "NO"
            stoch_note = (f"K{_st['k']:.0f}/D{_st['d']:.0f} | " + (_wr or ""))[:70].replace(",", ";")
            if trade_direction in STOCH_GATE_DIRECTIONS:
                _pass = (_dv or _wk) if STOCH_CONFIRM_MODE == "GATE_ANY" else (
                        _dv if STOCH_CONFIRM_MODE == "GATE_DIV" else True)
                if not _pass:
                    _dir_k = "LONG" if trade_direction == "BULLISH" else "SHORT"
                    print(f"  ⛔ [{label}] {pair} {_dir_k} — STOCH GATE ({STOCH_CONFIRM_MODE}): "
                          f"div={stoch_div_tag}, melemah={stoch_weak_tag}, skip")
                    return
        except Exception as _e_st:
            stoch_div_tag, stoch_weak_tag, stoch_note = "ERR", "ERR", str(_e_st)[:60].replace(",", ";")

    # Supporting
    vol_rat      = volume_ratio(df_entry)
    displacement = detect_displacement(df_entry, trade_direction)
    rsi          = calculate_rsi(df_entry)
    if SHORT_RSI_MAX_ENTRY > 0 and trade_direction == "BEARISH" and rsi >= SHORT_RSI_MAX_ENTRY:
        print(f"  ⛔ [{label}] {pair} SHORT — RSI entry {rsi:.1f} >= {SHORT_RSI_MAX_ENTRY:g} "
              f"(pullback, bukan lanjut turun), skip")
        return
    # 1:1 main73: indikator tambahan di bawah HANYA display/log — tidak boleh
    # menggagalkan sinyal kalau error (main73 tidak menghitungnya sama sekali).
    try:
        adx = calculate_adx(df_entry)
    except Exception:
        adx = 25.0
    try:
        macd_info = calculate_macd(df_entry)
    except Exception:
        macd_info = None
    try:
        levels_info = get_nearest_levels(df_entry, price)
    except Exception:
        levels_info = None
    m_pts, macro_reason = macro_score(pair, trade_direction, btc_bias, btcd_trend)
    ema_pts, ema_reason, ema20, ema50, ema200 = get_ema_score(df_entry, trade_direction)

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4b — BTC/BTCD LTF CORRELATION FILTER
    # Deteksi kondisi BTC + BTC Dominance di low timeframe sebelum sinyal keluar.
    # Blok sinyal jika kondisi makro LTF sangat berlawanan.
    # ─────────────────────────────────────────────────────────────────────────
    df_btc_ltf    = fetch_btc_ltf_data(BTC_LTF_TF, limit=60)
    btc_ltf_dir   = get_btc_ltf_direction(df_btc_ltf)
    df_btcd       = fetch_btcd_ohlcv(tf=BTCDOM_LTF_TF, limit=80)
    btcd_ltf_dir  = get_btcd_ltf_direction(df_btcd)

    corr_score, corr_reason, corr_blocked = analyze_btcd_correlation(
        direction    = trade_direction,
        btc_ltf_dir  = btc_ltf_dir,
        btcd_ltf_dir = btcd_ltf_dir,
        btc_bias_htf = btc_bias,
        btcd_trend_htf = btcd_trend,
        pair         = pair,
    )

    if corr_blocked:
        dir_str = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  🚫 [{label}] {pair} {dir_str} — BTCD CORR BLOK: {corr_reason}")
        return

    if _sz_short and SZ_REQUIRE_BTC_NOT_UP and btc_ltf_dir == "BULLISH":
        SZ_STATS["block_btc"] += 1
        print(f"  ⛔ [{label}] {pair} SHORT — SUPPLY: BTC {BTC_LTF_TF} sedang naik, skip")
        return

    # corr_score dimasukkan ke macro_pts sebagai bonus/penalty tambahan
    m_pts = m_pts + corr_score
    if corr_score != 0:
        macro_reason = f"{macro_reason} | {corr_reason}"

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5 — SCORING AKUMULATIF
    # ─────────────────────────────────────────────────────────────────────────
    score, score_bd = compute_score(
        htf_aligned      = htf_aligned,
        bos_confirmed    = bos_confirmed,
        bos_strength     = bos_str_label,
        momentum_aligned = momentum_align,
        momentum_present = momentum_any,
        liq_swept        = liq_swept,
        sweep_strength   = sweep_strength,
        in_ob            = (in_ob or _rz_touch),
        in_fvg           = in_fvg,
        displacement     = displacement,
        vol_rat          = vol_rat,
        session          = session,
        rsi              = rsi,
        macro_pts        = m_pts,
        ema_pts          = ema_pts,
        ref_aligned      = ref_aligned,
        adx              = adx,
    )

    # Threshold berdasarkan tier
    # /setscoreupto: kalau MIN_SCORE_CUSTOM > 0, nilai custom override default
    if MIN_SCORE_CUSTOM > 0:
        effective_gate = MIN_SCORE_RELAXED_CUSTOM if tier == "RELAXED" else MIN_SCORE_CUSTOM
    else:
        effective_gate = MIN_SCORE_RELAXED if tier == "RELAXED" else MIN_SCORE

    # ─────────────────────────────────────────────────────────────────────────
    # RR check (tetap diperlukan sebagai quality gate — bukan logika, tapi risk)
    # 1:1 PORT dari base signal-only (main41): entry dari best_ob/fvg di lokasi
    # harga SAAT INI (in_ob/in_fvg), tanpa entry-override / liquidity-TP-adjust
    # / max-entry-distance guard — komponen tsb tidak ada di algoritma base.
    # ─────────────────────────────────────────────────────────────────────────
    entry, sl, tp1, tp2, rr1, rr2 = calculate_rr(
        df_entry, trade_direction,
        best_ob if in_ob else None,
        fvg     if in_fvg else None,
    )
    if _dz_long:
        entry, sl, tp1, tp2, rr1, rr2 = _dz_entry_sl_tp(price, _rz_lo, _dz_wick, entry_atr)
    elif _sz_short:
        entry, sl, tp1, tp2, rr1, rr2 = _sz_entry_sl_tp(price, _rz_hi, _sz_wick, entry_atr)
    elif _rz_touch:
        entry, sl, tp1, tp2, rr1, rr2 = _rz_entry_sl_tp(trade_direction, price, _rz_lo, _rz_hi, entry_atr)

    if MAX_ENTRY_DISTANCE_ENABLED and price > 0:
        _dist_pct = abs(entry - price) / price
        if _dist_pct > MAX_ENTRY_DISTANCE_PCT:
            print(f"  ⛔ [{label}] {pair} — entry {_dist_pct*100:.2f}% dari harga "
                  f"> {MAX_ENTRY_DISTANCE_PCT*100:.1f}% (kejauhan), skip")
            return

    grade = grade_signal(rr1)
    if grade is None:
        print(f"  ⛔ [{label}] {pair} — RR {rr1} < {RR_GRADE_B} (tidak layak)")
        return

    if SIGNAL_MODE == "SNIPER" and grade != "A":
        print(f"  ⛔ [{label}] {pair} — SNIPER: Grade {grade} rejected")
        return

    # FIX (2026-08-31, 1:1 PORT dari base): RR "dead zone" reject — backtest
    # 444 trade nemu rr1 di rentang [2.5, 4.0) WR cuma 18-23% (-16.4R
    # gabungan), jauh lebih jelek daripada sweet spot RR 1.5-2.0 (WR 40%,
    # +25.7R). RR di zona ini ditolak.
    if 2.5 <= rr1 < 4.0:
        print(f"  ⛔ [{label}] {pair} — RR {rr1:.2f} di dead-zone [2.5, 4.0) (ditolak)")
        return

    # FIX (2026-08-31, 1:1 PORT dari base): reject sinyal BULLISH saat sesi
    # Asia — backtest nemu kombinasi ini WR 21.3% (-14.85R, n=47), jauh di
    # bawah rata-rata.
    if trade_direction == "BULLISH" and session == "Asia" and not RESEARCH_MODE:
        print(f"  ⛔ [{label}] {pair} — BULLISH di sesi Asia (ditolak, historis WR 21.3%)")
        return

    # FIX (2026-09-03, 1:1 PORT dari base): backtest 64 trade tier RELAXED
    # (2026-09-02/03) — SHORT WR 19% (27/35 total SL adalah SHORT) vs LONG
    # WR 45%. Sampai scoring engine di-retune dengan sampel lebih besar,
    # SHORT di RELAXED harus lolos gate dengan skor lebih tinggi (efektif
    # dikurangi SHORT_SCORE_PENALTY_RELAXED) supaya tidak asal entry melawan
    # bias market yang sedang dominan.
    effective_score = score
    if tier == "RELAXED" and trade_direction == "BEARISH":
        effective_score = score - SHORT_SCORE_PENALTY_RELAXED

    if STRUCT_FILTER_LTF and not _zone_rev:
        _sf_ok, _sf_why = _ltf_structure_ok(df_entry, trade_direction)
        if not _sf_ok:
            _dir_sf = "LONG" if trade_direction == "BULLISH" else "SHORT"
            print(f"  ⛔ [{label}] {pair} {_dir_sf} — STRUCTURE FILTER: {_sf_why}, skip")
            return

    if _dz_long:
        # MODE DEMAND: tidak pakai BOS/HH+HL. Cukup reaksi buyer + candle close bullish.
        _dz_ok, _dz_why = _demand_buyer_reaction(df_entry, _rz_lo, _rz_hi, entry_atr)
        if not _dz_ok:
            print(f"  ⛔ [{label}] {pair} LONG — DEMAND: {_dz_why}, skip")
            return
        DZ_STATS["after_bos"] += 1   # nama key dipertahankan (dibaca backtest) = lolos reaksi buyer
    elif _sz_short:
        # MODE SUPPLY: tidak pakai BOS/LH+LL. Cukup reaksi seller + candle close bearish.
        _dz_ok, _dz_why = _supply_seller_reaction(df_entry, _rz_lo, _rz_hi, entry_atr)
        if not _dz_ok:
            print(f"  ⛔ [{label}] {pair} SHORT — SUPPLY: {_dz_why}, skip")
            return
        SZ_STATS["after_bos"] += 1   # = lolos reaksi seller
    elif BOS_NONE_HARD_GATE and not bos_confirmed and not _rz_touch:
        _dir_b = "LONG" if trade_direction == "BULLISH" else "SHORT"
        print(f"  ⛔ [{label}] {pair} {_dir_b} — BOS NONE di {entry_tf} (hard gate, skip)")
        return

    if effective_score < effective_gate and not (_zone_rev and DZ_SKIP_SCORE_GATE):
        print(
            f"  📉 [{label}] {pair} — Score {score}"
            f"{f' (eff {effective_score} setelah SHORT penalty)' if effective_score != score else ''}"
            f" < {effective_gate} "
            f"| HTF:{score_bd['htf']} BOS:{score_bd['bos']} "
            f"Mom:{score_bd['momentum']} Liq:{score_bd['liquidity']} Zone:{score_bd['entry_zone']}"
        )
        return

    # ─────────────────────────────────────────────────────────────────────────
    # Build reasons
    # ─────────────────────────────────────────────────────────────────────────
    htf_label = f"HTF {htf_bias} ({htf_tf})" if htf_bias != "RANGING" \
        else f"HTF RANGING ({htf_tf}) → LTF {trade_direction}"
    reasons = [f"{htf_label} — {htf_event or 'trend confirmed'}"]

    if ranging_ctx is not None:
        reasons.append(f"🔀 Ranging adaptive: LTF score {ranging_ctx.ltf_score}/100")
        for r in ranging_ctx.reasons:
            reasons.append(f"  ↳ {r}")

    if ref_bias != "N/A":
        reasons.append(f"Ref TF ({mode.get('ref_tf')}) bias: {ref_bias} {'✅' if ref_aligned else '⚠️'}")
    reasons.append(f"EMA: {ema_reason}")

    if _dz_long:
        reasons.append(f"✅ Demand {'1W' if HTF_DEMAND_MODE == 2 else '1D'}: {_dz_why}")
    elif _sz_short:
        reasons.append(f"✅ Supply {SZ_TF_MAP.get(HTF_SUPPLY_MODE, '?').upper()} (OB bearish): {_dz_why}")
    elif bos_confirmed:
        reasons.append(f"BOS {trade_direction} terkonfirmasi ({bos_str_label}) pada {entry_tf}")
        if eng_reason and eng_ok:
            reasons.append(f"✅ {eng_reason}")
    else:
        reasons.append(f"⚠️ Tidak ada BOS pada {entry_tf} (score dikurangi)")

    if momentum_align:
        reasons.append(f"Momentum {ltf_momentum} konsisten ({LTF_CONSECUTIVE_MIN} candle) ✅")
    elif momentum_any:
        reasons.append(f"⚠️ Momentum ada tapi tidak konsisten ({ltf_momentum})")

    if liq_swept:
        lvl_str = f" @ {fmt_price(pair, liq_level)}" if liq_level else ""
        reasons.append(f"Liquidity: {liq_type}{lvl_str} [{sweep_strength}]")
    else:
        reasons.append("⚠️ Tidak ada liquidity sweep (score dikurangi)")

    if in_ob and best_ob:
        reasons.append(f"OB: ${fmt_price(pair, best_ob['low'])}–${fmt_price(pair, best_ob['high'])} (pullback ke OB midpoint)")
    if in_fvg and fvg:
        reasons.append(f"FVG: ${fmt_price(pair, fvg['bottom'])}–${fmt_price(pair, fvg['top'])} ({fvg['fill_pct']*100:.0f}% filled)")
    if in_ob and in_fvg:
        reasons.append("OB + FVG confluence ✅")
    if entry_eng_reason and entry_eng_ok:
        reasons.append(f"✅ {entry_eng_reason}")
    if not in_ob and not in_fvg:
        reasons.append("⚠️ Tidak di zona OB/FVG — entry market")

    if displacement:
        reasons.append("Displacement candle terkonfirmasi ✅")
    if vol_rat >= 1.5:
        reasons.append(f"Volume spike: {vol_rat}× avg ✅")
    if session in ("London", "New York"):
        reasons.append(f"Prime session: {session} ✅")
    reasons.append(f"RSI: {rsi:.1f} → {rsi_score(rsi):+} pts")
    _regime_label = "RANGING" if adx < ADX_RANGING_THRESHOLD else "TRENDING"
    # BUG FIX (2026-09-18): score_bd['regime'] sudah dihapus dari compute_score()
    # saat parity fix (regime_score tidak lagi dijumlah ke total skor), tapi
    # baris ini masih baca key itu → KeyError: 'regime' di HAMPIR SEMUA pair,
    # bikin analyze_pair() gagal total (except-block di caller skip pair
    # tanpa evaluasi lanjut). Ini penyebab utama sinyal jadi langka. ADX tetap
    # ditampilkan untuk info, tapi tanpa klaim poin skor (karena memang 0).
    reasons.append(f"ADX: {adx:.1f} ({_regime_label}) — info only, tidak pengaruhi skor")
    reasons.append(f"Macro: {macro_reason}")

    signal = {
        "pair":        pair,
        "direction":   "LONG" if trade_direction == "BULLISH" else "SHORT",
        "entry":       round(entry, 6),
        "stop_loss":   round(sl, 6),
        "take_profit": [round(tp1, 6), round(tp2, 6)],
        "RR":          rr1,
        "score":       score,
        "grade":       grade,
        "reason":      reasons,
        # ── Field tambahan untuk send_telegram_signal() (execute_trade) ──────
        "_score_bd":     score_bd,
        "_session":      session,
        "_pa_name":      "-",
        "_btc_bias":     btc_bias,
        "_btcd_trend":   btcd_trend,
        "_macro_reason": macro_reason,
        # ── Konteks market saat entry — dibawa sampai ke trade log ───────────
        "_atr_pct":        round(entry_atr_pct, 4),
        "_sl_atr_mult":    round(abs(entry - sl) / entry_atr, 3) if entry_atr > 0 else 0,
        "_vol_rat":        vol_rat,
        "_mkt_regime":     _mkt_regime,
        "_alt_bull_pct":   _alt_bull,
        "_alt_bear_pct":   _alt_bear,
        "_htf_bias":       htf_bias,
        "_bos_strength":   bos_str_label,
        "_engulf_break":   eng_tag,
        "_engulf_entry":   entry_tag,
        "_engulf_note":    eng_note,
        "_fvg_retest":     ret_tag,
        "_fvg_retest_note": ret_note,
        "_stoch_div":      stoch_div_tag,
        "_stoch_weak":     stoch_weak_tag,
        "_stoch_note":     stoch_note,
        "_sweep_strength": sweep_strength or "NONE",
        "_sl_source":      ("DEMAND" if _dz_long else ("SUPPLY" if _sz_short else ("HTFZ" if _rz_touch else ("OB" if in_ob else ("FVG" if in_fvg else "SWING"))))),
        "_rsi_entry":      round(rsi, 2),
        "_btc_ltf_dir":    btc_ltf_dir,
        "_btcd_ltf_dir":   btcd_ltf_dir,
        "_corr_reason":    corr_reason,
        # ── Technical Snapshot (display-only, lihat SECTION 10b) ─────────────
        "_macd":           macd_info,
        "_nearest_levels": levels_info,
    }

    # ── Gate jarak SL maksimum (lihat MAX_SL_ENTRY_PCT) ─────────────────────
    # Dipasang di analyze_pair supaya berlaku sama untuk bot live DAN backtest.py.
    try:
        _gate = float(MAX_SL_ENTRY_PCT or 0.0)
    except Exception:
        _gate = 0.0
    if _gate > 0 and entry > 0:
        _sl_gate_pct = abs(entry - sl) / entry
        if _sl_gate_pct > _gate:
            print(f"  ⛔ [{label}] {pair} — SL {_sl_gate_pct*100:.2f}% > batas "
                  f"{_gate*100:.2f}% (volatilitas tinggi), skip")
            return

    # ── Guard dedup sebelum masuk antrian ──────────────────────────────────
    # Cek di sini juga agar sinyal dari 4 mode berbeda (SWING/INTRADAY/SCALP/LOW_TF)
    # yang menghasilkan entry/SL hampir sama tidak masuk kandidat berkali-kali.
    _dir_chk = "LONG" if trade_direction == "BULLISH" else "SHORT"
    if _is_duplicate_signal(pair, _dir_chk, entry, sl):
        print(f"  ⏩ [{label}] {pair} {_dir_chk} — hash match, sudah pernah dikirim, skip")
        return

    if _dz_long:
        DZ_STATS["signals"] += 1
    if _sz_short:
        SZ_STATS["signals"] += 1
    signal_candidates.append({
        "pair":         pair,
        "direction":    trade_direction,
        "score":        score,
        "rr":           rr1,
        "signal":       signal,
        "mode":         mode,
        "score_bd":     score_bd,
        "session":      session,
        "rsi":          rsi,
        "btc_bias":     btc_bias,
        "btcd_trend":   btcd_trend,
        "mkt_regime":   _mkt_regime,
        "alt_bull_pct": _alt_bull,
        "alt_bear_pct": _alt_bear,
        "macro_reason": macro_reason,
        "entry":        entry,
        "sl":           sl,
        "tp1":          tp1,
        "tp2":          tp2,
        "tier":         tier,
        "ref_bias":     ref_bias,
        "ema_reason":   ema_reason,
        "ema20":        ema20,
        "ema50":        ema50,
        "ema200":       ema200,
        "ranging_ctx":  ranging_ctx,
        "btc_ltf_dir":  btc_ltf_dir,
        "btcd_ltf_dir": btcd_ltf_dir,
        "corr_reason":  corr_reason,
        # ── Konteks market saat entry — untuk analisis kausal trade log ──────
        # (kenapa SL kena: apa struktural lemah dari awal, apa SL kekecilan
        # relatif ke ATR saat itu, apa volume/momentum sedang lemah, dst.)
        "atr_pct":        round(entry_atr_pct, 4),
        "sl_atr_mult":    round(abs(entry - sl) / entry_atr, 3) if entry_atr > 0 else 0,
        "vol_rat":        vol_rat,
        "htf_bias":       htf_bias,
        "bos_strength":   bos_str_label,
        "engulf_break":   eng_tag,
        "engulf_entry":   entry_tag,
        "engulf_note":    eng_note,
        "fvg_retest":     ret_tag,
        "fvg_retest_note": ret_note,
        "stoch_div":      stoch_div_tag,
        "stoch_weak":     stoch_weak_tag,
        "stoch_note":     stoch_note,
        "sweep_strength": sweep_strength or "NONE",
        "sl_source":      ("DEMAND" if _dz_long else ("SUPPLY" if _sz_short else ("HTFZ" if _rz_touch else ("OB" if in_ob else ("FVG" if in_fvg else "SWING"))))),
        "btc_bias":       btc_bias,
        "btcd_trend":     btcd_trend,
        "btc_ltf_dir":    btc_ltf_dir,
        "btcd_ltf_dir":   btcd_ltf_dir,
        "corr_reason":    corr_reason,
        "adx_htf":        round(adx_htf, 2),
        "di_gap_htf":     round(di_gap_htf, 2),
        "ema_gap_htf":    round(ema_gap_htf, 3),
        "htf_struct_ok":  _hs_ok,
        "htf_er":         _hs_er,
    })

    tier_label = f"{tier}{'(RANGING)' if ranging_ctx else ''}"
    bos_tag    = "DEMAND:✅" if _dz_long else ("SUPPLY:✅" if _sz_short else (f"BOS:{bos_str_label}" if bos_confirmed else "BOS:❌"))
    liq_tag    = f"Liq:{'✅' if liq_swept else '❌'}"
    zone_tag   = "Zone:OB+FVG" if (in_ob and in_fvg) else ("Zone:OB" if in_ob else ("Zone:FVG" if in_fvg else "Zone:❌"))
    corr_em    = "✅" if corr_score >= 0 else "⚠️"
    print(
        f"  📋 [{label}/{tier_label}] {pair} — Queued | "
        f"Score:{score} Grade:{grade} RR:1:{rr1} | "
        f"{bos_tag} {liq_tag} {zone_tag} | "
        f"BTC_LTF:{btc_ltf_dir} BTCD_LTF:{btcd_ltf_dir} {corr_em}"
    )


def fetch_all_tf_for_pair(pair, htf_tfs, ref_tfs) -> dict:
    cache   = {}
    all_tfs = list(set(htf_tfs + ref_tfs))
    for tf in all_tfs:
        try:
            cache[tf] = fetch_ohlcv(pair, tf, limit=250)
            time.sleep(0.15)
        except Exception as e:
            print(f"  ⚠️  {pair} @ {tf}: {e}")
            cache[tf] = None
    return cache


# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 21b — RALLY EXHAUSTION GATE & AVERAGE MARKET RSI
# ═══════════════════════════════════════════════════════════════════════════

EXHAUSTION_STOCH_THRESHOLD = 78   # %K di atas ini = overbought zone
EXHAUSTION_STOCH_D_GAP     = 5    # %K - %D < 5 dan mengecil = momentum melemah
EXHAUSTION_ENABLED         = True # toggle on/off

# ── HTF STOCHASTIC GATE ──────────────────────────────────────────────────────
# Logika: Kalau Stoch H4/Daily masih OVERBOUGHT atau CROSSING_DOWN di atas 50,
#   → LONG diblok, SHORT diutamakan (probabilitas lebih tinggi untuk turun dulu)
# Kalau Stoch H4/Daily sudah OVERSOLD atau CROSSING_UP di bawah 50,
#   → SHORT diblok, LONG diutamakan (setup long probabilitas tinggi)
# Toggle: /togglehtfstochgate via Telegram (atau set di sini)
HTF_STOCH_GATE_ENABLED = True   # True = aktif | False = disable gate ini


def is_rally_exhausted() -> tuple:
    """
    Deteksi apakah rally BTC sudah exhaustion berdasarkan 3 konfirmasi:
    1. Stoch RSI %K > 78 (overbought zone)
    2. Gap %K - %D mengecil (momentum melemah)
    3. Candle BTC 1H terakhir punya upper wick rejection

    Return: (is_exhausted: bool, reason: str)
    """
    if not EXHAUSTION_ENABLED:
        return False, "Exhaustion gate disabled"
    try:
        df_btc = fetch_ohlcv_safe("BTC/USDT", "1h", limit=100)
    except Exception as e:
        return False, f"BTC fetch error: {e}"
    if df_btc is None or len(df_btc) < 20:
        return False, "Data BTC tidak cukup"

    k_now, d_now = calc_stoch_rsi(df_btc)

    # Hitung nilai sebelumnya (potong 1 candle terakhir)
    k_prev, d_prev = calc_stoch_rsi(df_btc.iloc[:-1])

    gap_now  = k_now - d_now
    gap_prev = k_prev - d_prev

    if k_now < EXHAUSTION_STOCH_THRESHOLD:
        return False, f"Stoch %K={k_now:.1f} belum overbought"

    momentum_weak = gap_now < gap_prev and gap_now < EXHAUSTION_STOCH_D_GAP

    last = df_btc.iloc[-1]
    o, h, cl = float(last["open"]), float(last["high"]), float(last["close"])
    body       = abs(cl - o)
    upper_wick = h - max(o, cl)
    wick_ratio = upper_wick / body if body > 1e-9 else 0
    rejection_candle = wick_ratio > 1.2 and cl < o

    if momentum_weak and rejection_candle:
        return True, (
            f"Exhaustion: Stoch %K={k_now:.1f} OB | "
            f"gap {gap_prev:.1f}→{gap_now:.1f} melemah | "
            f"BTC upper wick rejection {wick_ratio:.1f}x"
        )
    if momentum_weak:
        return True, (
            f"Exhaustion: Stoch %K={k_now:.1f} OB | "
            f"gap melemah {gap_prev:.1f}→{gap_now:.1f}"
        )
    return False, f"Stoch %K={k_now:.1f}, gap={gap_now:.1f} — belum exhausted"


def get_average_market_rsi(signal_candidates: list) -> float:
    """
    Hitung rata-rata RSI dari semua pair hasil scan.
    Dipakai sebagai patokan kondisi market keseluruhan setelah scan selesai.

    > 70 → Market overbought → blok sinyal LONG
    < 30 → Market oversold  → blok sinyal SHORT

    Return: float (default 50.0 jika tidak ada data)
    """
    rsi_values = [c["rsi"] for c in signal_candidates if c.get("rsi") and 0 < c["rsi"] <= 100]
    if not rsi_values:
        return 50.0
    return round(sum(rsi_values) / len(rsi_values), 2)


def get_market_breadth(candidates: list) -> str:
    """
    v28: Hitung arah mayoritas sinyal dalam satu scan cycle.

    Return:
      'BULLISH' — ≥ BREADTH_THRESHOLD kandidat adalah LONG
      'BEARISH' — ≥ BREADTH_THRESHOLD kandidat adalah SHORT
      'NEUTRAL' — tidak ada mayoritas jelas, atau data kurang
    """
    if not BREADTH_ENABLED:
        return "NEUTRAL"
    total = len(candidates)
    if total < BREADTH_MIN_SAMPLES:
        return "NEUTRAL"
    long_count  = sum(1 for c in candidates if c["direction"] == "BULLISH")
    short_count = sum(1 for c in candidates if c["direction"] == "BEARISH")
    long_ratio  = long_count  / total
    short_ratio = short_count / total
    if long_ratio  >= BREADTH_THRESHOLD:
        return "BULLISH"
    if short_ratio >= BREADTH_THRESHOLD:
        return "BEARISH"
    return "NEUTRAL"


# ██  SECTION 22b — STARTUP IP DETECTION & WHITELIST REMINDER
# ═══════════════════════════════════════════════════════════════════════════

def get_public_ip() -> str | None:
    """
    Ambil IP publik server/PC yang menjalankan bot.
    Coba beberapa layanan sebagai fallback.
    """
    services = [
        "https://api.ipify.org?format=text",
        "https://ifconfig.me/ip",
        "https://checkip.amazonaws.com",
        "https://icanhazip.com",
        "https://ident.me",
    ]
    for url in services:
        try:
            r = requests.get(url, timeout=5)
            ip = r.text.strip()
            if ip:
                return ip
        except Exception:
            continue
    return None


def send_startup_ip(max_retries: int = 5, wait_seconds: int = 30):
    """
    Saat bot start:
    1. Deteksi IP publik server ini
    2. Kirim ke Telegram supaya bisa di-whitelist di Binance API
    3. Countdown 30 detik — kasih waktu whitelist sebelum bot mulai trading
    4. Retry sampai 5x jika IP tidak terdeteksi
    """
    print("\n🌐 Mendeteksi IP publik server...")

    public_ip = None
    for attempt in range(1, max_retries + 1):
        public_ip = get_public_ip()
        if public_ip:
            print(f"✅ IP terdeteksi: {public_ip}")
            break
        print(f"  ⚠️  Percobaan {attempt}/{max_retries} gagal deteksi IP, coba lagi...")
        time.sleep(3)

    now_str = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")

    if public_ip:
        msg = (
            f"🌐 <b>IP Server Bot — {now_str}</b>\n"
            f"{'─'*38}\n"
            f"{_bot_version_line_html()}"
            f"{'─'*38}\n"
            f"📡 IP Publik   : <code>{public_ip}</code>\n"
            f"{'─'*38}\n"
            f"⚠️ <b>Jika pakai mode LIVE:</b>\n"
            f"Tambahkan IP ini ke whitelist API key Binance kamu:\n"
            f"  1. Buka <b>Binance → API Management</b>\n"
            f"  2. Edit API key kamu\n"
            f"  3. Tambahkan <code>{public_ip}</code> ke IP Restriction\n"
            f"  4. Simpan perubahan\n"
            f"{'─'*38}\n"
            f"⏳ Bot siap dalam <b>{wait_seconds} detik</b>...\n"
            f"Mode sekarang: <b>{'🔴 LIVE' if BOT_MODE == 'LIVE' else '🟢 DEMO (testnet)'}</b>"
        )
        print(f"\n{'─'*60}")
        print(f"📡 IP PUBLIK SERVER: {public_ip}")
        print(f"{'─'*60}")
        print(f"⚠️  WHITELIST IP INI DI BINANCE API MANAGEMENT JIKA PAKAI MODE LIVE!")
        print(f"{'─'*60}")
    else:
        msg = (
            f"⚠️ <b>Gagal Deteksi IP Server — {now_str}</b>\n"
            f"{'─'*38}\n"
            f"{_bot_version_line_html()}"
            f"{'─'*38}\n"
            f"Tidak bisa mendeteksi IP publik setelah {max_retries}x percobaan.\n"
            f"Kemungkinan tidak ada koneksi internet saat startup.\n"
            f"{'─'*38}\n"
            f"🔧 Cek manual IP kamu dan whitelist di Binance API jika mode LIVE.\n"
            f"⏳ Bot siap dalam <b>{wait_seconds} detik</b>...\n"
            f"Mode sekarang: <b>{'🔴 LIVE' if BOT_MODE == 'LIVE' else '🟢 DEMO (testnet)'}</b>"
        )
        print("❌ Gagal deteksi IP publik — bot tetap lanjut, cek internet kamu.")

    print(f"🏷  BOT_VERSION={BOT_VERSION} | BUILD_DATE={BUILD_DATE} | "
          f"GitHub={_bot_version_fields()['github_account']}")
    send_telegram_raw(msg)

    # Countdown — kasih waktu whitelist IP sebelum bot konek ke Binance
    print(f"\n⏳ Countdown {wait_seconds} detik sebelum bot mulai...")
    for remaining in range(wait_seconds, 0, -5):
        print(f"   {remaining}s ...")
        time.sleep(5)
    print("✅ Countdown selesai!\n")


# ═══════════════════════════════════════════════════════════════════════════
# ██  SECTION 22 — MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("🤖  BOT AUTO TRADE — BINANCE LIVE × PA + S&D + S/R ENGINE  V5")
    print("=" * 70)
    print(f"📊 Pairs       : {len(PAIR_LIST)}")
    print(f"🔢 Modes       : {len(MODES)}")
    for m in MODES:
        print(f"   • {m['label']:10s} HTF: {m['htf_tf']:4s} → Entry: {m['entry_tf']}")
    print(f"🧮 Score Gate  : ≥ {MIN_SCORE} pts")
    print(f"📐 RR Gate     : ≥ {RR_GRADE_B} (Grade B) | ≥ {RR_GRADE_A} (Grade A)")
    print(f"⏱  Cooldown    : {COOLDOWN_MINUTES} min per pair/mode/TF")
    print(f"⚡ Leverage    : Auto-tier (5x-20x by price) | Max Trades: {MAX_OPEN_TRADES} (dynamic, scale otomatis)")
    print(f"🚀 Scan Config : interval={SCAN_INTERVAL}s | workers={MAX_WORKERS}  ← VERIFIKASI VERSI DEPLOY DI SINI")
    print()
    print("TELEGRAM COMMANDS: /start /pnl /status /resumeorpause /closeallposition /changeliveordemo /setmarginratio /maxopentrade /maxtradesperday /pairs /resetpnl /cleanuporders /help")
    print("=" * 70)

    # Kirim IP publik ke Telegram + countdown 30 detik untuk whitelist
    send_startup_ip(max_retries=5, wait_seconds=30)

    # ── Load state dari disk (Railway Volume) ─────────────────────────────────
    # Semua setting (BOT_MODE, MARGIN_RATIO, dll) di-restore ke nilai terakhir
    # sebelum bot di-restart. Jika file tidak ada → pakai default di atas.
    load_state()

    # ── Auto-save background thread ───────────────────────────────────────────
    # Simpan state ke disk setiap 5 menit agar tidak hilang jika restart mendadak
    threading.Thread(target=_auto_save_loop, daemon=True).start()

    init_balance()
    load_exchange_info()
    if not _exchange_info_cache:
        print("⚠️  Exchange info kosong! Coba load ulang...")
        load_exchange_info()
    # sync_existing_positions() dipindah ke cmd_start() — jangan sync sebelum user konfirmasi mode
    # Mencegah false-close posisi LIVE karena bot sempat konek endpoint salah saat restart

    # ── Startup cleanup: tuntaskan pending limit order sisa sesi sebelumnya ──
    # Dipanggil DI SINI (setiap boot, sebelum loop PAUSED) supaya tidak ada
    # limit order + conditional order nyangkut yang bisa ke-fill diam-diam
    # selagi bot PAUSED menunggu /start. Lihat docstring fungsi untuk detail.
    try:
        startup_cleanup_pending_orders()
    except Exception as _su_err:
        print(f"⚠️ Startup cleanup pending orders gagal: {_su_err}")

    # ── Sapu bersih orphan orders lain (symbol yang sudah tidak ada posisi
    # maupun pending limit sama sekali — misal sisa dari versi PAIR_LIST lama
    # atau order manual yang lupa di-cancel) ─────────────────────────────────
    try:
        cleanup_stale_orders(dry_run=False, silent_if_clean=True)
    except Exception as _cs_err:
        print(f"⚠️ Startup cleanup_stale_orders gagal: {_cs_err}")

    # Jalankan Telegram command listener di background thread
    # ← HARUS sebelum notif standby, supaya bot bisa langsung terima /start
    tg_thread = threading.Thread(target=telegram_polling_loop, daemon=True)
    tg_thread.start()

    # ── FIX KRITIS: thread khusus TP1/TP2/Trailing BE ─────────────────────────
    # Jalan independen dari scan sinyal, polling tiap TRAILING_POLL_INTERVAL
    # detik (bukan menunggu SCAN_INTERVAL selesai). Lihat _trailing_monitor_loop.
    trailing_thread = threading.Thread(target=_trailing_monitor_loop, daemon=True)
    trailing_thread.start()
    print(f"✅ Trailing monitor thread aktif — cek TP1/TP2/BE tiap {TRAILING_POLL_INTERVAL}s")
    time.sleep(1)   # beri waktu thread polling siap

    # ── Notif standby — minta user kirim /start ──────────────────────────────
    try:
        bal     = get_total_balance()
        # FIX: handle None (API error saat startup)
        bal_str = f"{bal:.2f} USDT" if bal is not None else "N/A"
    except Exception:
        bal_str = "N/A"

    mode_em = "🔴 LIVE" if BOT_MODE == "LIVE" else "🟢 DEMO (testnet)"

    send_telegram_raw(
        f"⏸ <b>BOT STANDBY — Menunggu /start</b>\n"
        f"{'─'*38}\n"
        f"💼 Balance   : <b>{bal_str}</b>\n"
        f"🌐 Mode      : <b>{mode_em}</b>\n"
        f"⚡ Max SL/trade: <b>{MAX_SL_LOSS_PCT*100:.1f}%</b> dari balance | Max: <b>{MAX_OPEN_TRADES}</b> trades\n"
        f"🚀 Scan: interval={SCAN_INTERVAL}s, workers={MAX_WORKERS} <i>(cek versi deploy)</i>\n"
        f"{'─'*38}\n"
        f"⚠️ <b>Posisi belum di-sync</b> — sync akan dilakukan saat /start dikirim.\n"
        f"Pastikan mode sudah benar sebelum /start:\n"
        f"  /changeliveordemo — ganti LIVE/DEMO (saat ini: <b>{mode_em}</b>)\n"
        f"  /setmarginratio — set max SL loss % per trade\n"
        f"  /maxopentrade — ubah max posisi\n"
        f"  /pairs — atur pair yang dipantau\n"
        f"  /maxtradesperday — batas trade per hari\n"
        f"{'─'*38}\n"
        f"Ketik <b>/start</b> untuk konek ke Binance & mulai trading.\n"
        f"Ketik <b>/help</b> untuk daftar lengkap command."
    )
    print("⏸ Bot STANDBY — menunggu /start dari Telegram...")

    htf_tfs_needed = list({m["htf_tf"] for m in _ALL_MODES})  # pakai semua TF; difilter per scan

    while True:
        try:
            now     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            session = get_session()
            print(f"\n{'─'*70}")
            print(f"⏰ Scan: {now} UTC | Session: {session} | Source: {_active_data_source}")

            # ── Cek daily loss/win limit & reset hari baru ────────────────────
            # Dilakukan di setiap iterasi, sebelum cek bot_paused
            check_daily_limits()
            check_period_limits()

            if bot_paused:
                # ── PAUSED: tidak ada sync posisi sama sekali ─────────────────
                # SENGAJA tidak memanggil sync_closed_positions() / manage_trailing()
                # saat paused. Alasan:
                #   - Setelah restart Railway, BOT_MODE mungkin belum dikonfirmasi user
                #   - Jika mode=LIVE tapi koneksi ke testnet (atau sebaliknya),
                #     sync akan salah baca posisi → false stoploss / false close
                #   - User harus kirim /start dulu → baru sync posisi real dari Binance
                # Ini mencegah posisi LIVE dianggap close karena bot sempat konek ke DEMO.
                print("⏸ Bot PAUSED — menunggu /start (sync posisi ditunda)")
                time.sleep(SCAN_INTERVAL)
                continue

            check_drawdown()
            sync_closed_positions()          # ← deteksi posisi yang close dari luar
            monitor_pending_limit_orders()   # ← cek limit order: filled/timeout/cancel
            auto_cleanup_orphan_orders()      # ← auto bersihkan orphan SL/TP tiap 5 menit
            # FIX KRITIS: manage_trailing() TIDAK dipanggil di sini lagi.
            # ROOT CAUSE bug "TP1/TP2 tidak ngejual" & "trailing BE tidak ketrigger":
            # sebelumnya manage_trailing() cuma jalan SEKALI per SCAN_INTERVAL (60 detik),
            # SETELAH proses scan sinyal untuk puluhan pair selesai (bisa makan waktu lama
            # karena ThreadPoolExecutor fetch OHLCV banyak pair). Akibatnya jeda nyata
            # antar-pengecekan trailing/TP1/TP2 bisa jauh lebih dari 60 detik — harga
            # sempat lewat TP1/TP2 dan balik lagi ke SL sebelum sempat terdeteksi.
            # SEKARANG: manage_trailing() dijalankan di background thread terpisah
            # (lihat _trailing_monitor_loop di main()) yang polling tiap
            # TRAILING_POLL_INTERVAL detik, independen dari durasi scan sinyal.
            send_hourly_position_report()
            send_daily_summary()

            # ── Early exit: skip scan pair jika posisi aktif sudah penuh ────────
            # FIX BUG #5 (konsisten): pending limit juga dihitung agar tidak over-entry.
            _real_open = count_open_positions() + len(pending_limit_orders)
            _dyn_max   = get_dynamic_max_trades()
            if _real_open >= _dyn_max:
                print(f"  📛 Posisi + pending penuh ({_real_open}/{_dyn_max}) — skip scan, tunggu posisi close")
                time.sleep(300)  # tunggu 5 menit sebelum cek lagi
                continue

            # ═══════════════════════════════════════════════════════════════
            # ██  LANGKAH 1 — ANALISA BTC MULTI-TF (PRIORITAS TERTINGGI)
            #
            # BTC dianalisa LEBIH DULU sebelum scan altcoin apapun.
            # Urutan: Daily → H4 → H1, pakai Stochastic RSI 5,3,3 + Divergence
            #
            # ATURAN UTAMA:
            #   • BTC Daily RANGING        → SKIP SEMUA POSISI (tidak ada setup)
            #   • BTC Daily BULLISH        → Long alt diizinkan
            #   • BTC Daily BEARISH        → Short alt diizinkan
            #   • BTCD naik + BTC bearish  → Short tetap OK (alt season bearish)
            #   • BTCD turun + BTC bullish → Long tetap OK (alt season bullish)
            # ═══════════════════════════════════════════════════════════════
            # ── btc_bias: 1:1 DENGAN SIGNAL-ONLY (BASE) ──────────────────────
            # FIX GAP UTAMA: sebelumnya btc_bias diambil dari btc_mtf.bias
            # (analyze_btc_multitf — Daily→H4→H1 + Stoch RSI), sedangkan bot
            # signal-only pakai get_btc_bias() versi lama (1 TF, detect_structure
            # sederhana). Dua metode ini bisa hasilkan bias BERBEDA di momen yang
            # sama → btc_bias masuk ke macro_score() di dalam analyze_pair() →
            # skor akhir beda → sinyal bisa lolos di signal-only tapi tidak di
            # sini (atau sebaliknya), padahal analyze_pair() sendiri identik.
            # Sekarang btc_bias SELALU dari get_btc_bias() (1:1 base) — sama
            # persis dengan yang dipakai signal-only untuk analyze_pair().
            btc_bias = get_btc_bias()

            # 1:1 main73: analisa BTC Multi-TF (analyze_btc_multitf), Market Regime
            # dan HTF-Stoch-Gate log DIHAPUS dari loop scan. Ketiganya info/log
            # saja (tidak masuk skor/gate analyze_pair), tapi masing-masing memakan
            # banyak request Binance (BTC 1d/4h/1h + sampling banyak altcoin untuk
            # regime) lewat semaphore 1.5s -> scan makin lambat -> candle yang
            # dianalisa jadi telat dibanding main73.
            print(f"🎯 btc_bias DIPAKAI UNTUK ANALYZE_PAIR (1:1 main73, {BTC_BIAS_TF}): {btc_bias}")

            # ── v27: BTC RANGING tidak lagi memblok scan altcoin ─────────────
            # Altcoin punya struktur sendiri — BTC ranging bukan alasan skip.
            if btc_bias == "RANGING":
                print(f"\n⚠️  BTC RANGING — konteks BTC tidak jelas, tetap scan altcoin berdasarkan struktur pair masing-masing.")
            _btc_ranging_override = False   # v27: tidak pernah override pair list

            # ── BTC.D: fetch untuk filter korelasi ───────────────────────────────
            # 1:1 DENGAN BASE: btcd_trend selalu dari get_btcd_bias() (RISING/FALLING/FLAT @1h)
            btcd_trend = get_btcd_bias()
            print(f"🪙 BTC Bias Final: {btc_bias} | BTC.D ({BTCDOM_HTF_TF}): {btcd_trend}")

            signal_candidates = []
            candidates_lock   = threading.Lock()
            _active_modes     = get_active_modes()   # ambil mode aktif (bisa difilter via command)

            # OPT: pakai TF dari mode aktif saja (bukan _ALL_MODES)
            # Hemat request saat /scalpingonly atau /intradayonly aktif
            htf_tfs_active = list({m["htf_tf"] for m in _active_modes})
            ref_tfs_needed = list({m["ref_tf"] for m in _active_modes if m.get("ref_tf")})

            # Disamakan dengan bot signal-only: HTF & ref pakai 250 candle
            # (sebelumnya 150 demi hemat bandwidth — tapi ini mengubah hasil
            # deteksi swing/BOS/D&S Zone di HTF, jadi sinyal jadi beda dari
            # signal-only). Sekarang 1:1.
            _TF_LIMIT_MAP = {tf: 250 for tf in htf_tfs_active}
            for tf in ref_tfs_needed:
                _TF_LIMIT_MAP[tf] = 250

            def process_pair(pair):
                tf_cache = {}

                # ── Routing mode: LOW_TF (SMC) berlaku sama untuk SEMUA pair ─────
                # (dulu ada percabangan khusus XAUUSDT→GOLD_SCALP dengan TF
                # sendiri 30m/5m; GOLD_SCALP dan MA_PULLBACK sudah dihapus,
                # sekarang cuma satu mode SMC murni, tidak perlu routing terpisah.
                # BUG FIX (historis): percabangan lama ini sempat membuat
                # _pair_modes KOSONG untuk semua pair non-gold — karena
                # GOLD_MODES sempat berisi label mode yang sama dengan
                # _active_modes, jadi baris "exclude GOLD_MODES" tanpa sengaja
                # meng-exclude SATU-SATUNYA mode yang ada, bikin nyaris 0 sinyal.
                _pair_modes = _active_modes
                _pair_tfs   = list(set(htf_tfs_active + ref_tfs_needed))

                for tf in _pair_tfs:
                    try:
                        limit = _TF_LIMIT_MAP.get(tf, 250)
                        tf_cache[tf] = fetch_ohlcv_realdata(pair, tf, limit=limit)
                        time.sleep(0.4)
                    except Exception as e:
                        print(f"  ⚠️  {pair} @ {tf}: {e}")
                        tf_cache[tf] = None

                # ── Early skip: micro-price pair yang tidak bisa penuhi minNotional ──
                # FIX (Aug 2026, perf): sebelumnya panggil get_current_price(pair) di
                # sini — 1 API call network TERPISAH per pair per scan (lewat semaphore
                # global, +1.5s delay), padahal SIGNALONLY tidak punya cek ini sama
                # sekali. Sekarang pakai candle CLOSE terakhir dari tf_cache yang SUDAH
                # difetch di atas — tidak ada request baru sama sekali. get_lot_filters()
                # sudah cached dari load_exchange_info() jadi tetap tanpa network call.
                try:
                    _mp_df = next((tf_cache[tf] for tf in _pair_tfs if tf_cache.get(tf) is not None), None)
                    _mp_price = float(_mp_df["close"].iloc[-1]) if _mp_df is not None and len(_mp_df) else None
                    if _mp_price and _mp_price > 0:
                        _mp_filters      = get_lot_filters(pair)
                        _mp_lot_max      = _mp_filters.get("lot_maxQty", 0)
                        _mp_min_notional = _mp_filters.get("minNotional", 5.0)
                        _mp_cmult        = get_contract_multiplier(pair)
                        if _mp_lot_max > 0 and (_mp_lot_max * _mp_price * _mp_cmult) < _mp_min_notional:
                            print(
                                f"  ⏭️  [{pair}] SKIP SCAN micro-price: "
                                f"lot_max={_mp_lot_max} × price={_mp_price:.6g} "
                                f"= {_mp_lot_max * _mp_price * _mp_cmult:.2f} < minNotional={_mp_min_notional}"
                            )
                            return
                except Exception:
                    pass  # Jika gagal cek, lanjut scan normal — jangan blok pair

                local_candidates = []
                for mode in _pair_modes:
                    df_htf = tf_cache.get(mode["htf_tf"])
                    df_ref = tf_cache.get(mode["ref_tf"]) if mode.get("ref_tf") else None
                    if df_htf is None:
                        continue
                    try:
                        # SMC FLOW MURNI (1:1 reference bot) — signature analyze_pair
                        # disederhanakan, gate BTC sekarang di dalam fetch_btc_setup_data()
                        # (dipanggil langsung oleh analyze_pair, bukan lewat kwargs di sini).
                        analyze_pair(
                            pair              = pair,
                            mode              = mode,
                            df_htf            = df_htf,
                            df_ref            = df_ref,
                            btc_bias          = btc_bias,
                            btcd_trend        = btcd_trend,
                            session           = session,
                            signal_candidates = local_candidates,
                        )
                    except Exception as e:
                        print(f"  ❌ [{mode['label']}] {pair}: {e}")
                if local_candidates:
                    with candidates_lock:
                        signal_candidates.extend(local_candidates)

            active_pairs = get_active_pairs()   # ← super scalper: 25 pair; normal: full list
            # v27: selalu scan semua pair aktif — BTC ranging tidak membatasi
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(process_pair, pair): pair for pair in active_pairs}
                for future in as_completed(futures):
                    pair_done = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"  ❌ Thread error [{pair_done}]: {e}")

            # ── Dedup per pair: ambil score tertinggi ────────────────────────
            seen: dict = {}
            for cand in signal_candidates:
                dir_str  = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                key      = f"{cand['pair']}|{dir_str}"
                existing = seen.get(key)
                if not existing or cand["score"] > existing["score"] or                    (cand["score"] == existing["score"] and cand["rr"] > existing["rr"]):
                    seen[key] = cand
            signal_candidates = list(seen.values())

            # ── Correlation filter — 1:1 main73 ──────────────────────────────
            # main73 memanggil apply_correlation_filter() tepat setelah dedup
            # per-pair (1 sinyal terbaik per grup korelasi & arah). Di versi ini
            # fungsinya ada tapi TIDAK PERNAH dipanggil, jadi beberapa sinyal
            # pair berkorelasi lolos ke eksekusi padahal main73 menyaringnya.
            if signal_candidates:
                _before_corr = len(signal_candidates)
                signal_candidates = apply_correlation_filter(signal_candidates)
                _skipped_corr = _before_corr - len(signal_candidates)
                if _skipped_corr:
                    print(f"\n🔗 Correlation: {_before_corr} → {len(signal_candidates)} ({_skipped_corr} suppressed)")

            if signal_candidates:
                # ── Hitung slot kosong setelah scan semua pair selesai ────────
                # FIX BUG #5: pending limit orders HARUS dihitung sebagai slot terpakai.
                # Tanpa ini, bot bisa kirim sinyal melebihi _dyn_max karena pending
                # tidak kelihatan di count_open_positions().
                _cur_open   = count_open_positions() + len(pending_limit_orders)
                _dyn_max    = get_dynamic_max_trades()
                _slots_free = max(0, _dyn_max - _cur_open)

                print(f"  📊 Scan selesai — {len(signal_candidates)} sinyal ditemukan | "
                      f"Posisi aktif: {_cur_open}/{_dyn_max} | Pending: {len(pending_limit_orders)} | Slot kosong: {_slots_free}")

                _avail_pre = get_balance() if _slots_free > 0 else None
                if _slots_free == 0:
                    print(f"  🛑 Tidak ada slot kosong ({_cur_open}/{_dyn_max}) — semua sinyal dilewati")
                elif _avail_pre is not None and _avail_pre <= 0:
                    print(f"  🛑 Slot kosong {_slots_free} tapi available balance=0 (margin habis) — semua sinyal dilewati")
                else:
                    # ── Filter duplikat dulu sebelum sort ────────────────────
                    filtered_candidates = []
                    for cand in signal_candidates:
                        pair  = cand["pair"]
                        entry, sl = cand["entry"], cand["sl"]

                        # Cek pair sudah ada pending limit order (dari Pre-Zone Sniper
                        # atau scan sebelumnya) — entry price bisa beda tapi pair sama
                        if pair in pending_limit_orders:
                            dir_str = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                            pend_dir = pending_limit_orders[pair].get("direction", "")
                            print(f"  ⏩ SKIP → {pair} {dir_str} sudah ada pending limit order ({pend_dir})")
                            continue

                        if _is_duplicate_signal(pair, cand["direction"], entry, sl):
                            dir_str = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                            print(f"  ⏩ DUPLIKAT dibuang → {pair} {dir_str}")
                        else:
                            filtered_candidates.append(cand)

                    # ── Sort by score DESC, tiebreak by RR DESC ──────────────
                    filtered_candidates.sort(key=lambda c: (c["score"], c["rr"]), reverse=True)

                    # ── Ambil hanya sejumlah slot kosong (N terbaik) ─────────
                    selected = filtered_candidates[:_slots_free]

                    print(f"  ✅ Memilih {len(selected)} sinyal terbaik dari {len(filtered_candidates)} kandidat "
                          f"(slot kosong: {_slots_free})")
                    for i, cand in enumerate(selected, 1):
                        dir_str = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                        print(f"     #{i} {cand['pair']} {dir_str} | Score:{cand['score']} RR:1:{cand['rr']} Grade:{cand['signal']['grade']}")

                    # ── Eksekusi sinyal terpilih dengan limit order seperti biasa ──
                    for cand in selected:
                        sig        = cand["signal"]
                        pair       = cand["pair"]
                        entry, sl  = cand["entry"], cand["sl"]
                        dir_str    = "LONG" if cand["direction"] == "BULLISH" else "SHORT"
                        tier_label = cand["tier"]

                        print(f"  🚨 SIGNAL → {pair} {dir_str} | Score:{cand['score']} Grade:{sig['grade']} RR:1:{cand['rr']} Tier:{tier_label}")

                        execute_trade(sig, cand["mode"])
                        _register_signal_hash(pair, cand["direction"], entry, sl)
                        save_state()   # ← simpan signal hash ke disk
            else:
                print("  📭 Tidak ada sinyal pada scan ini")

            print_stats()
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            print("\n👋 Bot dihentikan manual.")
            send_telegram_raw("👋 <b>Bot dihentikan manual.</b>")
            save_state()   # ← simpan state sebelum keluar
            break
        except Exception as e:
            print(f"❌ Loop error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    # ── Instance lock — cegah dua bot jalan bersamaan ────────────────────────
    # Jika file .lock sudah ada dan proses lama masih hidup → tampilkan error & exit.
    # Ini mencegah double-response di Telegram (dua instance reply command yang sama).
    import os, sys, atexit

    LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")

    def _acquire_instance_lock():
        if os.path.exists(LOCK_FILE):
            try:
                with open(LOCK_FILE) as _lf:
                    old_pid = int(_lf.read().strip())
                # Cek apakah PID lama masih hidup
                try:
                    os.kill(old_pid, 0)   # signal 0 = cek eksistensi proses saja
                    # Proses masih hidup → tolak instance baru
                    print(f"\n{'='*60}")
                    print(f"❌  BOT SUDAH BERJALAN! (PID: {old_pid})")
                    print(f"    Matikan instance lama dulu sebelum jalankan bot baru.")
                    print(f"    Cara: tutup terminal bot lama, atau kill PID {old_pid}")
                    print(f"{'='*60}\n")
                    send_telegram_raw(
                        f"⚠️ <b>Instance Ganda Dicegah</b>\n"
                        f"Bot baru dicoba dijalankan tapi instance lama (PID {old_pid}) masih aktif.\n"
                        f"Matikan instance lama dulu!"
                    )
                    sys.exit(1)
                except OSError:
                    # PID tidak ada (proses lama sudah mati tapi lock file tertinggal) → lanjut
                    print(f"ℹ️  Lock file lama ditemukan (PID {old_pid} sudah tidak aktif) — melanjutkan...")
            except Exception:
                pass  # lock file rusak/kosong → abaikan

        # Tulis PID kita ke lock file
        with open(LOCK_FILE, "w") as _lf:
            _lf.write(str(os.getpid()))

        # Hapus lock file saat bot selesai (normal maupun crash)
        atexit.register(lambda: os.remove(LOCK_FILE) if os.path.exists(LOCK_FILE) else None)

    _acquire_instance_lock()

    try:
        main()
    except SystemExit:
        pass  # Clean shutdown (e.g. drawdown limit reached)
    except Exception as e:
        print("❌ ERROR:", e)
    finally:
        try:
            input("\nTekan ENTER untuk keluar...")
        except (EOFError, OSError, ValueError):
            pass  # non-interactive environment (Docker, screen, nohup) — skip input prompt
