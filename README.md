# DataCollection

Python scripts for collecting archived financial market data for current S&P 500 constituents.

## Providers

- Universe: Wikipedia S&P 500 constituents table, enriched with SEC ticker/exchange metadata.
- Prices: Yahoo Finance chart API daily OHLCV, adjusted close, dividends, and splits.
- Fundamentals: SEC EDGAR `companyfacts` XBRL API, saved as raw JSON and normalized long CSV.
- Technicals: Alpha Vantage technical indicator endpoints, plus local technical feature generation from archived OHLCV.
- Sentiment: Alpha Vantage `NEWS_SENTIMENT` endpoint.
- Optional Alpha Vantage fundamentals: `OVERVIEW`, `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`, `EARNINGS`.
- Ownership, insider activity, analyst ratings, and estimates: Yahoo Finance via `yfinance`, saved locally as quote-summary enrichment files.

SEC data has no API key requirement, but SEC fair-access rules require an identifying `User-Agent`. Alpha Vantage endpoints require an API key and are rate limited by plan, so the scripts sleep between calls and write one file per symbol for resumable collection.

## Setup

```bash
cd "/Users/soumyasen/Agentic AI/AGQ/SenQuant/DataCollection"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env`:

```bash
SEC_USER_AGENT="SenQuantDataCollection/0.1 your.email@example.com"
ALPHA_VANTAGE_API_KEY="your_key_here"
```

## Quick Smoke Test

Run two tickers first before launching a full S&P 500 collection:

```bash
python -m datacollection.cli universe
python -m datacollection.cli prices --symbols AAPL,MSFT --start 2020-01-01
python -m datacollection.cli sec-fundamentals --symbols AAPL,MSFT
python -m datacollection.cli local-technicals
```

Wrapper scripts are also available:

```bash
python scripts/collect_prices.py --symbols AAPL,MSFT --start 2020-01-01
python scripts/collect_sec_fundamentals.py --symbols AAPL,MSFT
python scripts/compute_local_technicals.py
```

## Full Collection

Core archive without Alpha Vantage:

```bash
python -m datacollection.cli all --start 1990-01-01
```

Include Alpha Vantage fundamentals, provider technicals, and sentiment:

```bash
python -m datacollection.cli all \
  --start 1990-01-01 \
  --include-alpha \
  --sleep 12.5 \
  --sentiment-time-from 20200101T0000
```

For Alpha Vantage, start with subsets because all S&P 500 symbols across fundamentals, technicals, and sentiment can require thousands of calls:

```bash
python -m datacollection.cli alpha-sentiment \
  --symbols AAPL,MSFT,NVDA \
  --time-from 20200101T0000 \
  --sleep 12.5
```

## Output Layout

```text
data/
  universe/sp500_constituents.csv
  prices/yahoo_daily/{SYMBOL}.csv
  prices/yahoo_daily/corporate_actions/{SYMBOL}.csv
  fundamentals/sec_companyfacts/raw/{SYMBOL}.json
  fundamentals/sec_companyfacts/long/{SYMBOL}.csv
  technicals/from_yahoo_daily/{SYMBOL}.csv
  alpha_vantage/fundamentals/{function}/{SYMBOL}.json
  alpha_vantage/technicals/{indicator}/{SYMBOL}.json
  alpha_vantage/sentiment/{SYMBOL}.json
  errors/{stage}.csv
```

The scripts skip existing per-symbol files by default. Use `--overwrite` when you intentionally want to refresh a stage.

## Useful Commands

Current S&P 500 universe:

```bash
python -m datacollection.cli universe
```

Historical daily prices:

```bash
python -m datacollection.cli prices --start 1990-01-01
```

SEC fundamentals:

```bash
python -m datacollection.cli sec-fundamentals
```

Alpha Vantage fundamentals:

```bash
python -m datacollection.cli alpha-fundamentals --functions OVERVIEW,INCOME_STATEMENT,BALANCE_SHEET,CASH_FLOW,EARNINGS
```

Alpha Vantage technicals:

```bash
python -m datacollection.cli alpha-technicals --functions SMA,EMA,RSI,MACD,BBANDS,ADX,OBV
```

Alpha Vantage sentiment:

```bash
python -m datacollection.cli alpha-sentiment --time-from 20200101T0000 --limit-per-symbol 1000
```

Yahoo ownership, insider, analyst, and estimate enrichment:

```bash
python -m datacollection.cli yahoo-enrichment --sleep 0.1
```

Local technical feature generation:

```bash
python -m datacollection.cli local-technicals
```

## Local Data Browser

The `app/` folder contains a local browser UI for exploring the collected `data/` folder. It provides a stock screener, sector/exchange/data-coverage filters, price/return/RSI/ownership/rating filters, insider-buy flags, daily/weekly/monthly/yearly charts, selectable technical metrics, SEC fundamentals charts, institutional owner tables, insider transaction tables, analyst ratings, price targets, and earnings/revenue estimates.

Run it from this folder:

```bash
source .venv/bin/activate
python app/server.py --port 8010
```

Then open:

```text
http://127.0.0.1:8010
```

## Render Deployment

This repo includes `render.yaml` for deploying the Python-backed browser as a Render Web Service from GitHub.

Render settings:

```text
Build Command: pip install -r requirements.txt
Start Command: python app/server.py --host 0.0.0.0 --port $PORT
Health Check Path: /health
```

The app reads data from `SENQUANT_DATA_ROOT` when set, otherwise it reads the local `data/` folder. The Render blueprint sets:

```text
SENQUANT_DATA_ROOT=/var/data/senquant/data
```

### Optional Local LLM Chat

The dashboard chat can use a private Ollama service for local LLM explanations while keeping the existing deterministic data assistant as a fallback. The app first retrieves relevant SenQuant rows, then sends only that compact local-data payload to Ollama. If Ollama is unavailable or slow, the normal local assistant answer is returned.

The included `render.yaml` defines a private Docker service named `senquant-ollama` with a persistent model disk and wires the web app to it through Render's private network:

```text
SENQUANT_OLLAMA_HOSTPORT=<private senquant-ollama host:port>
SENQUANT_OLLAMA_MODEL=llama3.2:1b
SENQUANT_OLLAMA_TIMEOUT_SECONDS=6
```

The Ollama service stores models under `/var/data/ollama/models` and pulls `OLLAMA_MODEL` on first boot. Start with `llama3.2:1b` for faster CPU responses. You can move to a larger model by changing both `OLLAMA_MODEL` on the private service and `SENQUANT_OLLAMA_MODEL` on the web service, then redeploying.

If creating the private service manually, keep Docker context as the repository root and set Dockerfile Path to `./deploy/ollama/Dockerfile`.

The local collected dataset is large and intentionally excluded from Git. On Render, attach or create the persistent disk from `render.yaml`, then populate `/var/data/senquant/data` with the same folder layout shown in "Output Layout". The app will boot without data for health checks, but the browser is only useful after the data folder is populated.

For custom domains, point GoDaddy DNS to the Render service after the temporary `onrender.com` URL works:

```text
A      @     216.24.57.1
CNAME  www   <your-render-service>.onrender.com
```

### Code and Data Sync

Render auto-deploys code when `main` is pushed to GitHub. To push committed code from this Mac automatically after every local commit, install the local Git hook:

```bash
scripts/install_auto_push_hook.sh
```

After that, this is enough for code changes:

```bash
git add .
git commit -m "Describe change"
```

The hook runs `git push origin main`, and Render redeploys from GitHub.

Data is intentionally not committed to GitHub because the local `data/` folder is large. To sync data changes to Render's persistent disk, add the Render SSH target to `.env`:

```bash
RENDER_SSH_TARGET="srv-xxxxx@ssh.virginia.render.com"
RENDER_DATA_ROOT="/var/data/senquant"
```

Then run:

```bash
scripts/sync_render_data.sh
```

In Render Shell, extract the uploaded archive:

```bash
cd /var/data/senquant
tar -xzf senquant-data.tgz
```

Check that the live app sees the data:

```bash
curl https://septumcapital.com/health
```

### Daily Market Refresh

The daily refresh updates the current S&P 500 universe, incrementally refetches recent Yahoo OHLCV rows, merges them into `data/prices/yahoo_daily`, recomputes local technicals, and writes a refresh marker so the web app reloads the updated data.

Run it manually:

```bash
scripts/daily_refresh.sh
```

Render runs the same refresh inside the web service process at 5:30 PM America/New_York on weekdays when this environment variable is enabled:

```text
SENQUANT_ENABLE_DAILY_REFRESH=true
```

This is intentionally not a separate Render Cron service because Render cron jobs cannot access persistent disks. The web service already has `/var/data/senquant` mounted, so it is the correct place to update the CSV data.

Install the local macOS schedule:

```bash
scripts/install_local_daily_refresh.sh
```

That installs a LaunchAgent that runs hourly. Before 3:30 PM local time and on weekends, the script exits without doing work. After 3:30 PM, it keeps retrying hourly until that market day's refresh succeeds, then skips the remaining hourly runs for that market date. Logs are written to:

```text
logs/daily-refresh.out.log
logs/daily-refresh.err.log
```

## Notes

- The S&P 500 has multiple share classes, so the constituents table can contain more than 500 rows.
- Tickers such as `BRK.B` and `BF.B` are normalized to `BRK-B` and `BF-B` for Yahoo and Alpha Vantage file/API calls.
- SEC `companyfacts` is XBRL filing data. The normalized CSV intentionally keeps accession numbers, filing dates, fiscal period fields, units, and frames so downstream research can handle restatements and point-in-time logic.
- Local technicals are computed from adjusted close where available. Use provider technical endpoints only when you specifically need Alpha Vantage's calculations.

## References

- [SEC EDGAR Data APIs](https://data.sec.gov/)
- [SEC API Documentation](https://www.sec.gov/edgar/sec-api-documentation)
- [Alpha Vantage API Documentation](https://www.alphavantage.co/documentation/)
- [S&P 500 constituents table](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)
