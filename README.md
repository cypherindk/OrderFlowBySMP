# Binance Futures Order Flow + Volume Profile -> Telegram Bot

Gerçek zamanlı order flow (imbalance, delta, absorbsiyon, divergence) ve
REST klines'tan hesaplanan hacim profili (POC/VAH/VAL/HVN/LVN) sinyallerini
Telegram'a gönderir. **Trade açmaz, sadece bilgilendirme amaçlıdır.**

## ⚠️ GitHub Actions ile 7/24 çalıştırma hakkında

GitHub Actions'ta bir job **en fazla 6 saat** çalışabiliyor. Bu yüzden bot
her ~6 saatte bir yeniden başlıyor (cron: `0 */6 * * *`), her başlangıçta
websocket'ler yeniden kurulur. Bu, birkaç dakikalık kısa kesintiler anlamına
gelir (sinyal kaçırma riski var, ama bot kalıcı state tutmadığı için sorun
yaratmaz).

**Repo public olmalı** — private repo'da ücretsiz Actions dakikası ayda 2000dk
(~33 saat) ile sınırlı, bu 24/7'ye yetmiyor. Public repo'da Actions dakikası
sınırsız ücretsiz. Secret'lar (token'lar) public repo'da da gizli kalır,
sadece kod görünür olur.

## Kurulum

### 1. Bu klasörü GitHub'a push et

```bash
cd repo
git init
git add .
git commit -m "Order flow bot ilk sürüm"
git branch -M main
git remote add origin https://github.com/<kullanici-adin>/<repo-adin>.git
git push -u origin main
```

(GitHub'da önce boş bir **public** repo oluşturman gerekiyor: github.com/new)

### 2. Telegram bot oluştur

1. Telegram'da **@BotFather**'a git, `/newbot` yaz, adım adım ilerle.
2. Sana verdiği **token**'ı kaydet.
3. Oluşturduğun bota Telegram'dan bir mesaj at (örn: "merhaba").
4. Tarayıcıda şu adrese git:
   `https://api.telegram.org/bot<TOKEN>/getUpdates`
5. Dönen JSON içindeki `"chat":{"id": ...}` değerini kaydet — bu senin **chat id**'n.

### 3. GitHub Secrets ekle

Repo sayfasında: **Settings → Secrets and variables → Actions → New repository secret**

Şunları ekle:
| Secret adı | Değer |
|---|---|
| `TELEGRAM_BOT_TOKEN` | BotFather'dan aldığın token |
| `TELEGRAM_CHAT_ID` | getUpdates'ten bulduğun chat id |
| `BINANCE_API_KEY` | (opsiyonel, sadece REST rate-limit için) |

### 4. Workflow'u çalıştır

- Otomatik: cron her 6 saatte bir tetikler, bir şey yapmana gerek yok.
- Manuel test: repo'da **Actions** sekmesi → **Order Flow Bot** → **Run workflow**
  (sembol, skor eşiği gibi parametreleri buradan değiştirebilirsin).

### 5. Logları görmek istersen

Actions sekmesinde ilgili run'a tıkla → "Botu çalıştır" adımının loglarını
görürsün (hangi sinyal ne zaman gönderildi, hata var mı vs).

## Parametreler

`orderflow_bot.py --help` ile tüm CLI argümanlarını görebilirsin. Önemli olanlar:

- `--symbol` : işlem çifti (örn. btcusdt, ethusdt)
- `--score-threshold` : -1..1 arası, alert için gereken minimum |skor|
- `--profile-interval` : hacim profili için kline periyodu (1m, 5m, 15m...)
- `--require-confluence` : sadece fiyat bir VP seviyesine (POC/VAH/VAL) yakınken alert gönder

## Sorumluluk reddi

Bu araç sadece analiz/bilgilendirme amaçlıdır, yatırım tavsiyesi değildir.
Otomatik emir açmaz/kapatmaz. Kullanmadan önce paper trading ile test edin.
