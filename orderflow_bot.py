"""
Kraken Futures Order Flow + Volume Profile -> Telegram Sinyal Botu
----------------------------------------------------------------------
Kraken Futures'ın PUBLIC REST + WebSocket API'lerini kullanır. Hesap/API
key GEREKMEZ. Kraken ABD'de yasal olarak hizmet verdiği için Binance/Bybit'in
aksine ABD merkezli IP'leri (GitHub Actions runner'ları dahil) bloklamaz.

Kraken'in trade feed'i gerçek taker yönünü (`side: buy`/`sell`) doğrudan
veriyor -- Binance'teki gibi OHLC'den tahmin etmeye gerek yok.

Kurulum:
    pip install websockets requests

Çalıştırma:
    export TELEGRAM_BOT_TOKEN="123456:ABC..."
    export TELEGRAM_CHAT_ID="123456789"
    python orderflow_bot.py --symbol PI_XBTUSD --score-threshold 0.6

Sembol notu: PI_XBTUSD = Bitcoin perpetual (inverse). Güncel sembol listesi
için: https://futures.kraken.com/derivatives/api/v3/instruments

Bu script sadece analiz/alert amaçlıdır, otomatik emir GÖNDERMEZ.
"""

import asyncio
import json
import time
import os
import argparse
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List

import requests
import websockets

# ================== AYARLAR ==================

KRAKEN_FUTURES_WS = "wss://futures.kraken.com/ws/v1"
KRAKEN_FUTURES_REST = "https://futures.kraken.com/derivatives/api/v3"


@dataclass
class Config:
    symbol: str = "PI_XBTUSD"
    depth_levels: int = 20                      # imbalance için üst N seviye
    eval_interval_sec: float = 5.0               # sinyal değerlendirme sıklığı
    delta_window_sec: int = 60                   # delta/divergence lookback penceresi
    absorption_window_sec: int = 15              # absorbsiyon tespiti penceresi
    absorption_price_range_pct: float = 0.05     # % - bu aralığın altı "dar range" sayılır
    combined_score_threshold: float = 0.6        # -1..1, alert eşiği
    alert_cooldown_sec: int = 120                # aynı yönde tekrar alert bekleme süresi
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # ---- Hacim Profili (REST trade history tabanlı, gerçek buy/sell) ----
    profile_max_trades: int = 2000          # profil için toplanacak toplam trade sayısı
    profile_max_pages: int = 25             # /history sayfa başı 100 trade, max sayfa
    profile_bins: int = 24                  # fiyat aralığı sayısı
    profile_refresh_sec: int = 300          # profili kaç saniyede bir yeniden hesapla
    va_percent: float = 70.0                # value area %
    hvn_mult: float = 1.5                   # HVN eşik çarpanı (ortalamaya göre)
    lvn_mult: float = 0.5                   # LVN eşik çarpanı (ortalamaya göre)
    price_proximity_pct: float = 0.15       # fiyat bir VP seviyesine bu % kadar yakınsa "teyit"
    require_level_confluence: bool = False  # True ise sadece VP seviyesine yakınken alert


CFG = Config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("orderflow")

# ================== VERİ YAPILARI ==================

@dataclass
class Trade:
    ts: float
    price: float
    qty: float
    side: str   # "buy" ya da "sell" - Kraken'in verdiği gerçek taker yönü


@dataclass
class State:
    trades: Deque[Trade] = field(default_factory=lambda: deque(maxlen=20000))
    local_bids: Dict[float, float] = field(default_factory=dict)   # price -> qty
    local_asks: Dict[float, float] = field(default_factory=dict)   # price -> qty
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
        else:
            log.info("Telegram mesajı başarıyla gönderildi (chat_id=%s)", CFG.telegram_chat_id)
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
        if t.side == "buy":
            buy_vol += t.qty
        else:
            sell_vol += t.qty
    return buy_vol - sell_vol


def compute_imbalance() -> float:
    """Orderbook imbalance: -1 (satış baskın) .. +1 (alış baskın)."""
    if not STATE.local_bids or not STATE.local_asks:
        return 0.0
    top_bids = sorted(STATE.local_bids.items(), key=lambda kv: -kv[0])[:CFG.depth_levels]
    top_asks = sorted(STATE.local_asks.items(), key=lambda kv: kv[0])[:CFG.depth_levels]
    bid_vol = sum(q for _, q in top_bids)
    ask_vol = sum(q for _, q in top_asks)
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

    buy_vol = sum(t.qty for t in window_trades if t.side == "buy")
    sell_vol = sum(t.qty for t in window_trades if t.side == "sell")
    total_vol = buy_vol + sell_vol
    if total_vol == 0:
        return 0.0

    if price_range_pct > CFG.absorption_price_range_pct:
        return 0.0  # dar range değil, absorbsiyon sayılmaz

    dominant_side = (buy_vol - sell_vol) / total_vol
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
        b = sum(t.qty for t in trades if t.side == "buy")
        s = sum(t.qty for t in trades if t.side == "sell")
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

# ================== HACİM PROFİLİ (REST /history tabanlı, gerçek buy/sell) ==================

def fetch_trade_history_page(symbol: str, last_time: str = None) -> list:
    url = f"{KRAKEN_FUTURES_REST}/history"
    params = {"symbol": symbol}
    if last_time:
        params["lastTime"] = last_time
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("history", [])


def fetch_recent_trades_sync(symbol: str, max_trades: int, max_pages: int) -> list:
    """Kraken /history sayfa başı 100 trade döner, geriye doğru sayfalayarak toplar."""
    all_trades: list = []
    last_time = None
    for _ in range(max_pages):
        page = fetch_trade_history_page(symbol, last_time)
        if not page:
            break
        all_trades.extend(page)
        if len(all_trades) >= max_trades:
            break
        last_time = page[-1].get("time")   # liste zamana göre azalan sıralı
        time.sleep(0.2)   # nazik rate-limit
    return all_trades[:max_trades]


def compute_volume_profile_from_trades(trades: list, num_bins: int, va_percent: float,
                                        hvn_mult: float, lvn_mult: float) -> dict:
    """
    POC/VAH/VAL/HVN/LVN hesaplar. Binance versiyonundan farkı: burada her
    trade tek bir fiyat noktası (candle range değil), bu yüzden binleme
    doğrudan indeks hesabıyla yapılıyor.
    """
    prices = [float(t["price"]) for t in trades]
    if not prices:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "hvn": [], "lvn": [], "hi": 0.0, "lo": 0.0}

    hi = max(prices)
    lo = min(prices)
    bin_size = (hi - lo) / num_bins if num_bins > 0 and hi > lo else 0.0

    buy_bins = [0.0] * num_bins
    sell_bins = [0.0] * num_bins

    for t in trades:
        price = float(t["price"])
        qty = float(t.get("size", t.get("qty", 0.0)))
        side = t.get("side")
        if bin_size > 0:
            b = int((price - lo) / bin_size)
            b = min(max(b, 0), num_bins - 1)
        else:
            b = 0
        if side == "buy":
            buy_bins[b] += qty
        else:
            sell_bins[b] += qty

    total_bins = [buy_bins[i] + sell_bins[i] for i in range(num_bins)]
    total_vol = sum(total_bins)
    if total_vol == 0:
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
    """Periyodik olarak REST'ten gerçek trade geçmişi çekip hacim profilini hesaplar."""
    while True:
        try:
            trades = await asyncio.to_thread(
                fetch_recent_trades_sync, CFG.symbol, CFG.profile_max_trades, CFG.profile_max_pages)
            if len(trades) >= 20:
                profile = compute_volume_profile_from_trades(
                    trades, CFG.profile_bins, CFG.va_percent, CFG.hvn_mult, CFG.lvn_mult)
                STATE.vp_poc = profile["poc"]
                STATE.vp_vah = profile["vah"]
                STATE.vp_val = profile["val"]
                STATE.vp_hvn = profile["hvn"]
                STATE.vp_lvn = profile["lvn"]
                STATE.vp_updated_ts = time.time()
                log.info("Hacim profili güncellendi (%d trade): POC=%.2f VAH=%.2f VAL=%.2f (HVN=%d, LVN=%d)",
                          len(trades), profile["poc"], profile["vah"], profile["val"],
                          len(profile["hvn"]), len(profile["lvn"]))
            else:
                log.warning("Hacim profili için yeterli trade verisi yok (%d)", len(trades))
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

def build_signal_message(direction: int, result: dict, lvl_info: dict, confluence: bool) -> str:
    side_txt = "LONG (Alış) 🟢" if direction == 1 else "SHORT (Satış) 🔴"

    if lvl_info["name"] is not None:
        level_line = (f"— En yakın VP seviyesi: {lvl_info['name']} "
                      f"({lvl_info['level']:.2f}, {lvl_info['dist_pct']:.2f}% uzaklıkta)"
                      + (" ✅ teyit" if confluence else ""))
    else:
        level_line = "— VP seviyesi henüz hesaplanmadı"

    return (
        f"<b>{CFG.symbol} Order Flow Sinyali (Kraken Futures)</b>\n"
        f"Yön: {side_txt}\n"
        f"Fiyat: {result['price']:.2f}\n"
        f"Skor: {result['score']:.2f}\n"
        f"— Imbalance: {result['imbalance']:.2f}\n"
        f"— Delta({CFG.delta_window_sec}s): {result['delta']:.2f} (norm {result['delta_norm']:.2f})\n"
        f"— Absorbsiyon: {result['absorption']:.2f}\n"
        f"— Delta Divergence: {result['divergence']:.2f}\n"
        f"{level_line}\n"
        f"— VP: POC {STATE.vp_poc:.2f} | VAH {STATE.vp_vah:.2f} | VAL {STATE.vp_val:.2f}\n"
        f"\n⚠️ Bu otomatik bir sinyaldir, yatırım tavsiyesi değildir."
    )


async def signal_loop():
    loop_count = 0
    while True:
        await asyncio.sleep(CFG.eval_interval_sec)
        loop_count += 1

        if STATE.last_price == 0:
            continue

        result = compute_combined_score()
        score = result["score"]

        # Her ~60 saniyede bir (eval_interval_sec=5 varsayılanla 12 döngü) anlık durumu logla,
        # eşik geçilmese bile ne olduğunu görebilesin.
        if loop_count % max(int(60 / CFG.eval_interval_sec), 1) == 0:
            log.info(
                "DURUM: fiyat=%.2f skor=%.2f (imbalance=%.2f delta_norm=%.2f absorb=%.2f diverg=%.2f) "
                "trade_sayisi=%d bid_seviye=%d ask_seviye=%d",
                result["price"], score, result["imbalance"], result["delta_norm"],
                result["absorption"], result["divergence"], len(STATE.trades),
                len(STATE.local_bids), len(STATE.local_asks),
            )

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

        msg = build_signal_message(direction, result, lvl_info, confluence)
        log.info("SİNYAL: %s", msg.replace("\n", " | "))
        send_telegram(msg)

# ================== WEBSOCKET DİNLEYİCİ ==================

def handle_trade_item(item: dict) -> None:
    try:
        trade = Trade(
            ts=float(item["time"]) / 1000.0,
            price=float(item["price"]),
            qty=float(item["qty"]),
            side=item.get("side", "buy"),
        )
        STATE.trades.append(trade)
        STATE.last_price = trade.price
    except (KeyError, ValueError, TypeError) as e:
        log.debug("trade parse hatası: %s", e)


def handle_book_snapshot(msg: dict) -> None:
    try:
        STATE.local_bids = {float(b["price"]): float(b["qty"]) for b in msg.get("bids", [])}
        STATE.local_asks = {float(a["price"]): float(a["qty"]) for a in msg.get("asks", [])}
    except (KeyError, ValueError, TypeError) as e:
        log.debug("book_snapshot parse hatası: %s", e)


def handle_book_delta(msg: dict) -> None:
    try:
        side = msg.get("side")
        price = float(msg["price"])
        qty = float(msg["qty"])
        book = STATE.local_bids if side == "buy" else STATE.local_asks
        if qty == 0:
            book.pop(price, None)
        else:
            book[price] = qty
    except (KeyError, ValueError, TypeError) as e:
        log.debug("book delta parse hatası: %s", e)


async def stream_listener():
    sub_trade = json.dumps({"event": "subscribe", "feed": "trade", "product_ids": [CFG.symbol]})
    sub_book = json.dumps({"event": "subscribe", "feed": "book", "product_ids": [CFG.symbol]})

    while True:
        try:
            async with websockets.connect(KRAKEN_FUTURES_WS, ping_interval=15, ping_timeout=10) as ws:
                await ws.send(sub_trade)
                await ws.send(sub_book)
                log.info("WebSocket bağlantısı kuruldu: %s (symbol=%s)", KRAKEN_FUTURES_WS, CFG.symbol)

                async for raw in ws:
                    msg = json.loads(raw)
                    feed = msg.get("feed")
                    event = msg.get("event")

                    if feed == "trade_snapshot":
                        for item in msg.get("trades", []):
                            handle_trade_item(item)
                    elif feed == "trade":
                        handle_trade_item(msg)
                    elif feed == "book_snapshot":
                        handle_book_snapshot(msg)
                    elif feed == "book":
                        handle_book_delta(msg)
                    elif event == "subscribed":
                        log.info("Abone olundu: feed=%s", msg.get("feed"))
                    elif event == "error":
                        log.error("WS hata mesajı: %s", msg.get("message"))

        except Exception as e:
            log.error("WebSocket hatası, 5sn sonra yeniden bağlanılıyor: %s", e)
            await asyncio.sleep(5)

# ================== GİRİŞ NOKTASI ==================

async def main():
    parser = argparse.ArgumentParser(description="Kraken Futures Order Flow -> Telegram Bot")
    parser.add_argument("--symbol", default=CFG.symbol,
                         help="Kraken Futures sembolü, örn: PI_XBTUSD, PI_ETHUSD")
    parser.add_argument("--score-threshold", type=float, default=CFG.combined_score_threshold)
    parser.add_argument("--require-confluence", action="store_true",
                         help="Sadece fiyat bir VP seviyesine (POC/VAH/VAL) yakınken alert gönder")
    parser.add_argument("--send-test-signal", action="store_true",
                         help="Sahte ama gerçekçi bir sinyal mesajı gönderip çık (websocket'e bağlanmaz)")
    args = parser.parse_args()

    CFG.symbol = args.symbol
    CFG.combined_score_threshold = args.score_threshold
    CFG.require_level_confluence = args.require_confluence

    if args.send_test_signal:
        # Sahte ama gerçekçi bir sonuç seti - gerçek sinyal mesajının nasıl göründüğünü test etmek için.
        STATE.vp_poc = 64007.94
        STATE.vp_vah = 64025.62
        STATE.vp_val = 63990.25
        fake_result = {
            "price": 64012.50,
            "score": 0.72,
            "imbalance": 0.65,
            "delta": 128.4,
            "delta_norm": 0.58,
            "absorption": 0.30,
            "divergence": 0.15,
        }
        fake_lvl_info = {"name": "POC", "level": STATE.vp_poc, "dist_pct": 0.01}
        msg = build_signal_message(direction=1, result=fake_result, lvl_info=fake_lvl_info, confluence=True)
        log.info("TEST SİNYALİ gönderiliyor: %s", msg.replace("\n", " | "))
        send_telegram(msg)
        return

    log.info("Başlatılıyor: symbol=%s, threshold=%.2f, require_confluence=%s",
              CFG.symbol, CFG.combined_score_threshold, CFG.require_level_confluence)

    send_telegram(
        f"🤖 Bot başladı: <b>{CFG.symbol}</b> (Kraken Futures)\n"
        f"Skor eşiği: {CFG.combined_score_threshold}\n"
        f"Bu bir test mesajıdır, Telegram bağlantısının çalıştığını doğrular."
    )

    await asyncio.gather(
        stream_listener(),
        signal_loop(),
        profile_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
