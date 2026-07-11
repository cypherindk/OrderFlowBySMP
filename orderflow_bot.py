"""
Binance Futures Order Flow + Volume Profile -> Telegram Sinyal Botu
----------------------------------------------------------------------
Gerçek zamanlı emir akışı (aggTrade) ve orderbook (depth) verisiyle
imbalance, absorbsiyon ve delta divergence hesaplar; buna ek olarak
REST klines üzerinden POC/VAH/VAL/HVN/LVN içeren bir hacim profili
oluşturur. Fiyat bu seviyelere yakınken order flow sinyali teyit
ediliyorsa Telegram'a zenginleştirilmiş bir alert gönderir.

Kurulum:
    pip install websockets requests

Çalıştırma:
    export TELEGRAM_BOT_TOKEN="123456:ABC..."
    export TELEGRAM_CHAT_ID="123456789"
    # opsiyonel (gerekli değil, sadece REST rate-limit için ekleniyor):
    export BINANCE_API_KEY="..."
    python orderflow_bot.py --symbol btcusdt --score-threshold 0.6

Not: Binance Futures public marketdata stream'leri (aggTrade, depth, klines)
API key GEREKTİRMEZ. BINANCE_API_KEY tamamen opsiyoneldir, verilirse sadece
REST isteklerine X-MBX-APIKEY header'ı olarak eklenir. Bu script sadece
analiz/alert amaçlıdır, otomatik emir GÖNDERMEZ, trade AÇMAZ/KAPATMAZ.
"""

import asyncio
import json
import time
import os
import argparse
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Tuple

import requests
import websockets

# ================== AYARLAR ==================

BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"


@dataclass
class Config:
    symbol: str = "btcusdt"
    depth_levels: int = 20                     # depth20 stream
    eval_interval_sec: float = 5.0              # sinyal değerlendirme sıklığı
    delta_window_sec: int = 60                  # delta/divergence lookback penceresi
    absorption_window_sec: int = 15             # absorbsiyon tespiti penceresi
    absorption_price_range_pct: float = 0.05    # % - bu aralığın altı "dar range" sayılır
    combined_score_threshold: float = 0.6       # -1..1, alert eşiği
    alert_cooldown_sec: int = 120               # aynı yönde tekrar alert bekleme süresi
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ---- Hacim Profili (REST klines tabanlı) ----
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")   # opsiyonel, sadece header için
    profile_interval: str = "1m"           # klines interval
    profile_limit: int = 500               # kaç mum kullanılacak (500x1m ~ 8 saat)
    profile_bins: int = 24                 # fiyat aralığı sayısı
    profile_refresh_sec: int = 60          # profili kaç saniyede bir yeniden hesapla
    va_percent: float = 70.0               # value area %
    hvn_mult: float = 1.5                  # HVN eşik çarpanı (ortalamaya göre)
    lvn_mult: float = 0.5                  # LVN eşik çarpanı (ortalamaya göre)
    price_proximity_pct: float = 0.15      # fiyat bir VP seviyesine bu % kadar yakınsa "teyit" sayılır
    require_level_confluence: bool = False  # True ise sadece VP seviyesine yakınken alert gönder


CFG = Config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("orderflow")

# ================== VERİ YAPILARI ==================

@dataclass
class Trade:
    ts: float
    price: float
    qty: float
    is_buyer_maker: bool   # True ise satıcı agresif (sell), False ise alıcı agresif (buy)


@dataclass
class State:
    trades: Deque[Trade] = field(default_factory=lambda: deque(maxlen=20000))
    best_bid: float = 0.0
    best_ask: float = 0.0
    bid_levels: List[Tuple[float, float]] = field(default_factory=list)
    ask_levels: List[Tuple[float, float]] = field(default_factory=list)
    last_price: float = 0.0
    last_alert_ts: float = 0.0
    last_alert_dir: int = 0   # 1 = long, -1 = short

    # ---- Hacim Profili seviyeleri (periyodik REST ile güncellenir) ----
    vp_poc: float = 0.0
    vp_vah: float = 0.0
    vp_val: float = 0.0
    vp_hvn: List[float] = field(default_factory=list)
    vp_lvn: List[float] = field(default_factory=list)
    vp_updated_ts: float = 0.0


STATE = State()

# ================== TELEGRAM ==================

def send_telegram(text: str) -> None:
    if not CFG.telegram_bot_token or not CFG.telegram_chat_id:
        log.warning("Telegram token/chat_id ayarlanmamış, mesaj gönderilmedi:\n%s", text)
        return
    url = f"https://api.telegram.org/bot{CFG.telegram_bot_token}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": CFG.telegram_chat_id,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=10)
        if r.status_code != 200:
            log.error("Telegram gönderim hatası: %s", r.text)
    except Exception as e:
        log.error("Telegram gönderim istisnası: %s", e)

# ================== SİNYAL HESAPLAMALARI ==================

def compute_delta(window_sec: int) -> float:
    """Verilen pencere içindeki net delta (buy_vol - sell_vol)."""
    cutoff = time.time() - window_sec
    buy_vol = 0.0
    sell_vol = 0.0
    for t in reversed(STATE.trades):
        if t.ts < cutoff:
            break
        if t.is_buyer_maker:
            sell_vol += t.qty   # buyer maker => aggressor sattı
        else:
            buy_vol += t.qty    # buyer taker => aggressor aldı
    return buy_vol - sell_vol


def compute_imbalance() -> float:
    """Orderbook imbalance: -1 (satış baskın) .. +1 (alış baskın)."""
    bid_vol = sum(q for _, q in STATE.bid_levels[:CFG.depth_levels])
    ask_vol = sum(q for _, q in STATE.ask_levels[:CFG.depth_levels])
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / total


def compute_absorption() -> float:
    """
    Yüksek hacim + dar fiyat aralığı = absorbsiyon.
    Bir taraf agresif işlem yapıyor ama fiyat kırılamıyorsa, karşı taraf
    absorbe ediyor demektir -> sinyal karşı yönde verilir.
    Dönüş: -1..1 (pozitif = bullish absorbsiyon, negatif = bearish absorbsiyon)
    """
    cutoff = time.time() - CFG.absorption_window_sec
    window_trades = [t for t in STATE.trades if t.ts >= cutoff]
    if len(window_trades) < 10:
        return 0.0

    prices = [t.price for t in window_trades]
    price_range_pct = (max(prices) - min(prices)) / min(prices) * 100 if min(prices) > 0 else 0

    buy_vol = sum(t.qty for t in window_trades if not t.is_buyer_maker)
    sell_vol = sum(t.qty for t in window_trades if t.is_buyer_maker)
    total_vol = buy_vol + sell_vol
    if total_vol == 0:
        return 0.0

    if price_range_pct > CFG.absorption_price_range_pct:
        return 0.0  # dar range değil, absorbsiyon sayılmaz

    dominant_side = (buy_vol - sell_vol) / total_vol
    # agresif taraf kazanamadıysa (fiyat sıkışmış), karşı taraf absorbe ediyor demektir.
    return -dominant_side


def compute_delta_divergence(window_sec: int) -> float:
    """
    Fiyat ve kümülatif delta yön uyumsuzluğu.
    Pencereyi ikiye böler, ilk yarı ve ikinci yarıda fiyat değişimi ile
    delta değişimini karşılaştırır.
    Dönüş: -1..1 (pozitif = bullish divergence, negatif = bearish divergence)
    """
    cutoff = time.time() - window_sec
    window_trades = [t for t in STATE.trades if t.ts >= cutoff]
    if len(window_trades) < 20:
        return 0.0

    mid = len(window_trades) // 2
    first_half = window_trades[:mid]
    second_half = window_trades[mid:]

    def price_change(trades):
        return trades[-1].price - trades[0].price if len(trades) >= 2 else 0.0

    def delta(trades):
        b = sum(t.qty for t in trades if not t.is_buyer_maker)
        s = sum(t.qty for t in trades if t.is_buyer_maker)
        return b - s

    price_chg2 = price_change(second_half)
    delta1 = delta(first_half)
    delta2 = delta(second_half)

    if price_chg2 > 0 and delta2 < delta1:
        strength = min(1.0, (delta1 - delta2) / (abs(delta1) + 1e-9))
        return -strength  # bearish divergence: fiyat yükseliyor, alım gücü zayıflıyor
    if price_chg2 < 0 and delta2 > delta1:
        strength = min(1.0, (delta2 - delta1) / (abs(delta1) + 1e-9))
        return strength   # bullish divergence: fiyat düşüyor, satım gücü zayıflıyor
    return 0.0


def normalize(value: float, scale: float) -> float:
    return max(-1.0, min(1.0, value / scale)) if scale else 0.0


def compute_combined_score() -> dict:
    imbalance = compute_imbalance()
    delta_raw = compute_delta(CFG.delta_window_sec)
    absorption = compute_absorption()
    divergence = compute_delta_divergence(CFG.delta_window_sec)

    recent_vols = [t.qty for t in STATE.trades if t.ts >= time.time() - CFG.delta_window_sec]
    scale = (sum(recent_vols) / max(len(recent_vols), 1)) * 50 if recent_vols else 1.0
    delta_norm = normalize(delta_raw, scale)

    # Ağırlıklar: imbalance %30, delta %30, absorbsiyon %20, divergence %20
    score = (imbalance * 0.30) + (delta_norm * 0.30) + (absorption * 0.20) + (divergence * 0.20)
    score = max(-1.0, min(1.0, score))

    return {
        "score": score,
        "imbalance": imbalance,
        "delta": delta_raw,
        "delta_norm": delta_norm,
        "absorption": absorption,
        "divergence": divergence,
        "price": STATE.last_price,
    }

# ================== HACİM PROFİLİ (REST klines tabanlı) ==================

def fetch_klines_sync(symbol: str, interval: str, limit: int) -> List[dict]:
    """Binance Futures public klines endpoint'i (API key gerektirmez)."""
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    headers = {}
    if CFG.binance_api_key:
        headers["X-MBX-APIKEY"] = CFG.binance_api_key  # opsiyonel, sadece rate-limit için

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    raw = r.json()

    candles = []
    for k in raw:
        volume = float(k[5])
        buy_vol = float(k[9])   # taker buy base asset volume -> gerçek alıcı hacmi
        candles.append({
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": volume,
            "buy_vol": buy_vol,
            "sell_vol": max(volume - buy_vol, 0.0),
        })
    return candles


def compute_volume_profile(candles: List[dict], num_bins: int, va_percent: float,
                            hvn_mult: float, lvn_mult: float) -> dict:
    """
    Pine Script'teki POC/VAH/VAL/HVN/LVN mantığının Python karşılığı.
    Fark: buy/sell ayrımı klines'in gerçek 'taker buy volume' alanından
    geliyor (OHLC yaklaşıklaması değil, gerçek veri).
    """
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    hi = max(highs)
    lo = min(lows)
    bin_size = (hi - lo) / num_bins if num_bins > 0 else 0.0

    buy_bins = [0.0] * num_bins
    sell_bins = [0.0] * num_bins

    for c in candles:
        h, l = c["high"], c["low"]
        vbuy, vsell = c["buy_vol"], c["sell_vol"]
        for b in range(num_bins):
            bin_low = lo + b * bin_size
            bin_high = bin_low + bin_size
            if h >= bin_low and l <= bin_high:
                buy_bins[b] += vbuy
                sell_bins[b] += vsell

    total_bins = [buy_bins[i] + sell_bins[i] for i in range(num_bins)]
    total_vol = sum(total_bins)
    if total_vol == 0 or num_bins == 0:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "hvn": [], "lvn": [], "hi": hi, "lo": lo}

    max_total = max(total_bins)
    poc_index = total_bins.index(max_total)
    poc_level = lo + poc_index * bin_size + bin_size / 2

    # ---- Value Area (VAH/VAL) ----
    target_vol = total_vol * (va_percent / 100)
    upper_idx = poc_index
    lower_idx = poc_index
    va_vol = total_bins[poc_index]

    while va_vol < target_vol and (upper_idx < num_bins - 1 or lower_idx > 0):
        vol_up = total_bins[upper_idx + 1] if upper_idx < num_bins - 1 else -1.0
        vol_down = total_bins[lower_idx - 1] if lower_idx > 0 else -1.0
        if vol_up >= vol_down:
            upper_idx += 1
            va_vol += vol_up
        else:
            lower_idx -= 1
            va_vol += vol_down

    vah = lo + (upper_idx + 1) * bin_size
    val = lo + lower_idx * bin_size

    # ---- HVN / LVN ----
    avg_vol = total_vol / num_bins
    hvn_levels: List[float] = []
    lvn_levels: List[float] = []
    for b in range(1, num_bins - 1):
        vB, vPrev, vNext = total_bins[b], total_bins[b - 1], total_bins[b + 1]
        mid = lo + b * bin_size + bin_size / 2
        if vB > vPrev and vB > vNext and vB > avg_vol * hvn_mult:
            hvn_levels.append(mid)
        if vB < vPrev and vB < vNext and vB < avg_vol * lvn_mult:
            lvn_levels.append(mid)

    return {"poc": poc_level, "vah": vah, "val": val, "hvn": hvn_levels,
            "lvn": lvn_levels, "hi": hi, "lo": lo}


async def profile_loop():
    """Periyodik olarak REST'ten kline çekip hacim profilini yeniden hesaplar."""
    while True:
        try:
            candles = await asyncio.to_thread(
                fetch_klines_sync, CFG.symbol, CFG.profile_interval, CFG.profile_limit)
            if candles:
                profile = compute_volume_profile(
                    candles, CFG.profile_bins, CFG.va_percent, CFG.hvn_mult, CFG.lvn_mult)
                STATE.vp_poc = profile["poc"]
                STATE.vp_vah = profile["vah"]
                STATE.vp_val = profile["val"]
                STATE.vp_hvn = profile["hvn"]
                STATE.vp_lvn = profile["lvn"]
                STATE.vp_updated_ts = time.time()
                log.info("Hacim profili güncellendi: POC=%.2f VAH=%.2f VAL=%.2f (HVN=%d, LVN=%d)",
                          profile["poc"], profile["vah"], profile["val"],
                          len(profile["hvn"]), len(profile["lvn"]))
        except Exception as e:
            log.error("Hacim profili güncelleme hatası: %s", e)
        await asyncio.sleep(CFG.profile_refresh_sec)


def nearest_level_info(price: float) -> dict:
    """Fiyata en yakın VP seviyesini (POC/VAH/VAL/HVN/LVN) ve uzaklığını % olarak döner."""
    levels = {}
    if STATE.vp_poc:
        levels["POC"] = STATE.vp_poc
    if STATE.vp_vah:
        levels["VAH"] = STATE.vp_vah
    if STATE.vp_val:
        levels["VAL"] = STATE.vp_val
    for i, lvl in enumerate(STATE.vp_hvn):
        levels[f"HVN{i + 1}"] = lvl
    for i, lvl in enumerate(STATE.vp_lvn):
        levels[f"LVN{i + 1}"] = lvl

    if not levels or price == 0:
        return {"name": None, "level": None, "dist_pct": None}

    name, level = min(levels.items(), key=lambda kv: abs(kv[1] - price))
    dist_pct = abs(level - price) / price * 100
    return {"name": name, "level": level, "dist_pct": dist_pct}

# ================== SİNYAL DEĞERLENDİRME DÖNGÜSÜ ==================

async def signal_loop():
    while True:
        await asyncio.sleep(CFG.eval_interval_sec)
        if STATE.last_price == 0:
            continue

        result = compute_combined_score()
        score = result["score"]

        direction = 0
        if score >= CFG.combined_score_threshold:
            direction = 1
        elif score <= -CFG.combined_score_threshold:
            direction = -1

        if direction == 0:
            continue

        lvl_info = nearest_level_info(result["price"])
        confluence = (lvl_info["dist_pct"] is not None and
                      lvl_info["dist_pct"] <= CFG.price_proximity_pct)

        if CFG.require_level_confluence and not confluence:
            continue

        now = time.time()
        same_dir_recent = (direction == STATE.last_alert_dir and
                            (now - STATE.last_alert_ts) < CFG.alert_cooldown_sec)
        if same_dir_recent:
            continue

        STATE.last_alert_ts = now
        STATE.last_alert_dir = direction

        side_txt = "LONG (Alış) 🟢" if direction == 1 else "SHORT (Satış) 🔴"

        if lvl_info["name"] is not None:
            level_line = (f"— En yakın VP seviyesi: {lvl_info['name']} "
                          f"({lvl_info['level']:.2f}, {lvl_info['dist_pct']:.2f}% uzaklıkta)"
                          + (" ✅ teyit" if confluence else ""))
        else:
            level_line = "— VP seviyesi henüz hesaplanmadı"

        msg = (
            f"<b>{CFG.symbol.upper()} Order Flow Sinyali</b>\n"
            f"Yön: {side_txt}\n"
            f"Fiyat: {result['price']:.2f}\n"
            f"Skor: {score:.2f}\n"
            f"— Imbalance: {result['imbalance']:.2f}\n"
            f"— Delta({CFG.delta_window_sec}s): {result['delta']:.2f} (norm {result['delta_norm']:.2f})\n"
            f"— Absorbsiyon: {result['absorption']:.2f}\n"
            f"— Delta Divergence: {result['divergence']:.2f}\n"
            f"{level_line}\n"
            f"— VP: POC {STATE.vp_poc:.2f} | VAH {STATE.vp_vah:.2f} | VAL {STATE.vp_val:.2f}\n"
            f"\n⚠️ Bu otomatik bir sinyaldir, yatırım tavsiyesi değildir."
        )
        log.info("SİNYAL: %s", msg.replace("\n", " | "))
        send_telegram(msg)

# ================== WEBSOCKET DİNLEYİCİLER ==================

async def stream_listener():
    streams = f"{CFG.symbol}@aggTrade/{CFG.symbol}@depth{CFG.depth_levels}@100ms"
    url = f"{BINANCE_FUTURES_WS}?streams={streams}"

    while True:
        try:
            async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
                log.info("WebSocket bağlantısı kuruldu: %s", url)
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data", {})
                    stream = msg.get("stream", "")

                    if stream.endswith("@aggTrade"):
                        handle_agg_trade(data)
                    elif "@depth" in stream:
                        handle_depth(data)

        except Exception as e:
            log.error("WebSocket hatası, 5sn sonra yeniden bağlanılıyor: %s", e)
            await asyncio.sleep(5)


def handle_agg_trade(data: dict) -> None:
    try:
        trade = Trade(
            ts=data["T"] / 1000.0,
            price=float(data["p"]),
            qty=float(data["q"]),
            is_buyer_maker=bool(data["m"]),
        )
        STATE.trades.append(trade)
        STATE.last_price = trade.price
    except (KeyError, ValueError) as e:
        log.debug("aggTrade parse hatası: %s", e)


def handle_depth(data: dict) -> None:
    try:
        bids = data.get("b", [])
        asks = data.get("a", [])
        STATE.bid_levels = [(float(p), float(q)) for p, q in bids]
        STATE.ask_levels = [(float(p), float(q)) for p, q in asks]
        if STATE.bid_levels:
            STATE.best_bid = STATE.bid_levels[0][0]
        if STATE.ask_levels:
            STATE.best_ask = STATE.ask_levels[0][0]
    except (KeyError, ValueError) as e:
        log.debug("depth parse hatası: %s", e)

# ================== GİRİŞ NOKTASI ==================

async def main():
    parser = argparse.ArgumentParser(description="Binance Futures Order Flow -> Telegram Bot")
    parser.add_argument("--symbol", default=CFG.symbol, help="Örn: btcusdt, ethusdt")
    parser.add_argument("--score-threshold", type=float, default=CFG.combined_score_threshold)
    parser.add_argument("--profile-interval", default=CFG.profile_interval,
                         help="Hacim profili için kline periyodu, örn: 1m, 5m, 15m")
    parser.add_argument("--require-confluence", action="store_true",
                         help="Sadece fiyat bir VP seviyesine yakınken alert gönder")
    args = parser.parse_args()

    CFG.symbol = args.symbol.lower()
    CFG.combined_score_threshold = args.score_threshold
    CFG.profile_interval = args.profile_interval
    CFG.require_level_confluence = args.require_confluence

    log.info("Başlatılıyor: symbol=%s, threshold=%.2f, profile_interval=%s, require_confluence=%s",
              CFG.symbol, CFG.combined_score_threshold, CFG.profile_interval,
              CFG.require_level_confluence)

    await asyncio.gather(
        stream_listener(),
        signal_loop(),
        profile_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
