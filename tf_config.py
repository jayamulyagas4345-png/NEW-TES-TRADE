"""
tf_config.py — semua parameter Trend Following V1.0 (4H regime + 1H pullback continuation).

Dipakai bersama oleh trend_follow.py (sinyal), tf_backtest.py (simulasi) dan nanti jalur live.
Ubah angka di sini, bukan di dalam kode. Tidak ada parameter yang dioptimasi: semua default
berasal dari spesifikasi dan DITETAPKAN sebelum melihat hasil backtest.
"""

STRATEGY = "TREND_FOLLOWING"

# ── Timeframe ────────────────────────────────────────────────────────────────
TREND_TIMEFRAME = "4h"
ENTRY_TIMEFRAME = "1h"

# ── Indikator ────────────────────────────────────────────────────────────────
EMA_FAST = 20
EMA_MID = 50
EMA_SLOW = 200
RSI_PERIOD = 14
RSI_LONG_MIN = 50          # LONG butuh RSI > 50
RSI_SHORT_MAX = 50         # SHORT butuh RSI < 50
ADX_PERIOD = 14
ADX_MIN = 20
ATR_PERIOD = 14

# ── Pullback / entry ─────────────────────────────────────────────────────────
PULLBACK_LOOKBACK = 6              # harga harus memasuki area EMA20–EMA50 dalam N candle 1H terakhir
PULLBACK_MAX_BREAK_ATR = 0.5       # batal jika ada close lebih dari 0.5 ATR di luar sisi jauh area EMA
MAX_DIST_FROM_EMA20_ATR = 1.5      # jangan mengejar: close maks 1.5 ATR dari EMA20
MAX_ENTRY_GAP_ATR = 0.5            # open candle entry maks 0.5 ATR dari close candle konfirmasi

# ── Stop loss (swing + ATR) ──────────────────────────────────────────────────
SWING_K = 3                        # pivot butuh 3 candle di kiri & kanan (terkonfirmasi setelah 3 candle)
SWING_MAX_AGE = 48                 # swing maksimal 48 candle 1H lalu
SL_ATR_BUFFER = 0.25
SL_MIN_ATR = 1.0                   # jarak SL minimal 1 ATR dari entry
SL_MAX_ATR = 2.5                   # jarak SL maksimal 2.5 ATR; di luar itu: SKIP

# ── Take profit (satu TP final) ──────────────────────────────────────────────
RISK_REWARD = 2.0

# ── Risk management ──────────────────────────────────────────────────────────
RISK_PER_TRADE = 0.005             # 0.5% equity
MAX_RISK_PER_TRADE = 0.01          # batas keras 1%
MAX_OPEN_POSITIONS = 3
MAX_TOTAL_OPEN_RISK = 0.015        # total risiko semua posisi terbuka ≤ 1.5% equity
MAX_SAME_DIRECTION = 2             # batasi eksposur searah (korelasi tinggi antar pair)
DAILY_LOSS_LIMIT = 0.02            # 2% (hari UTC) → stop entry baru
WEEKLY_LOSS_LIMIT = 0.05           # 5% (minggu ISO) → stop entry baru
LEVERAGE = 3
MARGIN_MODE = "ISOLATED"

# ── Exit tambahan ────────────────────────────────────────────────────────────
TIME_EXIT_HOURS = 24
TIME_EXIT_MIN_PROFIT_R = 0.5
EXIT_ON_REGIME_REVERSAL = True

# ── Biaya (backtest) ─────────────────────────────────────────────────────────
FEE_RATE = 0.0004                  # taker 0.04% per sisi
SLIPPAGE = 0.0002                  # 0.02% per sisi, melawan posisi

# ── Universe ─────────────────────────────────────────────────────────────────
PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"]

# ── Mode ─────────────────────────────────────────────────────────────────────
PAPER_TRADING = True               # True = jangan kirim order sungguhan

# ── Target evaluasi (kriteria lolos; ditetapkan sebelum pengujian) ───────────
TARGET_PROFIT_FACTOR = 1.5
TARGET_MAX_DRAWDOWN = 0.10
TARGET_MIN_TRADES = 200


def as_dict():
    return {k: v for k, v in globals().items() if k.isupper()}
