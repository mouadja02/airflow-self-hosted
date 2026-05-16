# Airflow Self-Hosted Infrastructure

A production-ready Apache Airflow deployment for personal data engineering projects, optimized for Raspberry Pi and self-hosted environments with automated CI/CD.

## Overview

This project provides a **complete self-hosted Airflow infrastructure** deployed on Raspberry Pi. It orchestrates multiple data pipelines covering Bitcoin on-chain analytics, market data, news, technical indicators, documentation scraping, and dataset publishing.

### Why Self-Hosted Airflow?

- **Cost-Effective**: Run on Raspberry Pi (~$100) instead of cloud services ($100+/month)
- **Full Control**: Complete control over your data pipeline infrastructure
- **Privacy**: Keep your data and workflows on your own hardware
- **Always On**: 24/7 pipeline execution without cloud bills

### Key Features

- **Production-Ready**: Battle-tested deployment with error handling and monitoring
- **Automated CI/CD**: GitHub Actions pipeline for testing and deployment
- **Docker-Based**: Containerized deployment with Docker Compose
- **ARM-Optimized**: Performance tuned for Raspberry Pi 5
- **Secure Remote Access**: Cloudflare Tunnel integration for secure SSH
- **Snowflake Integration**: Native support for Snowflake data warehouse
- **Multi-Source Data**: APIs, web scraping, and vector embeddings

---

## Current DAGs

### Bitcoin On-Chain & Price Data

| DAG | File | Schedule | Description |
|-----|------|----------|-------------|
| `btc_updater_batch1–4` | `dataset_updater.py` | 06:45–10:00 UTC | 32 on-chain metrics (MVRV, NUPL, SOPR, hashrate, ETF flows, etc.) fetched from bitcoin-data.com and merged into Snowflake `BITCOIN_DATA.DATA` |
| `bitcoin_ohlcv_dataset` | `bitcoin_ohlcv_dataset.py` | Daily 00:05 UTC | Bitcoin hourly OHLCV from Binance API with historical backfill to 2010 |
| `btc_technical_indicators_dataset` | `btc_dataset_github_sync_dag.py` | Daily 00:30 UTC | Refreshes Bitcoin OHLCV + 90+ technical indicators and publishes to GitHub dataset repo |
| `technical_indicators` | `technical_indicators_dag.py` | Daily | Calculates 110+ TA-Lib technical indicators on OHLCV data in Snowflake |
| `bitcoin_on_chain_trader` | `on_chain_trader_dag.py` | Daily 23:05 UTC | Computes on-chain trading signals (yfinance + web scraping) for algorithmic strategy |

### Market & Derivatives Data

| DAG | File | Schedule | Description |
|-----|------|----------|-------------|
| `financial_market_data` | `financial_market_data_dag.py` | Daily 22:30 UTC | NASDAQ, VIX, DXY, and Gold prices for BTC correlation analysis |
| `btc_funding_rate_updater` | `funding_rate_updater.py` | Every 8h (02:00/10:00/18:00 UTC) | Bitcoin perpetual funding rates with full datetime precision |
| `btc_open_interest_updater` | `OpenInterestFutures_data_updater_dag.py` | Daily | Bitcoin open futures interest from Binance, Bybit, OKX, Deribit, BitMEX, Huobi, Bitfinex, and more |

### News & Sentiment

| DAG | File | Schedule | Description |
|-----|------|----------|-------------|
| `bitcoin_news` | `bitcoin_news_dag.py` | Daily | Bitcoin news from The Guardian (historical backfill + delta), Finnhub, and AlphaVantage NEWS_SENTIMENT (multi-key rotation) |

### Documentation Scrapers / RAG Knowledge Bases

| DAG | File | Schedule | Description |
|-----|------|----------|-------------|
| `snowflake_docs_scraper` | `snowflake_docs_db_dag.py` | Weekly Sun 02:00 UTC | Scrapes Snowflake docs, generates OpenAI embeddings, stores in Pinecone for SnowPro Core exam RAG |
| `aws_saa_docs_scraper` | `aws_saa_docs_scraper.py` | Weekly | Scrapes AWS SAA documentation, generates embeddings, stores in Pinecone for SAA certification RAG |
| `dbt_iics_docs_scraper` | `iics_dbt_doc_scraper.py` | Weekly | Scrapes dbt and IICS documentation for a RAG agent focused on IICS-to-dbt migration |

### Infrastructure / Utilities

| DAG | File | Schedule | Description |
|-----|------|----------|-------------|
| `snowflake_trial_migration` | `snowflake_migrate_dag.py` | Manual | Migrates databases from an expiring Snowflake trial account to a new one |

---

## Project Structure

```
airflow-self-hosted/
├── dags/
│   ├── dataset_updater.py                    # 32 on-chain metrics → Snowflake (4 batches)
│   ├── bitcoin_ohlcv_dataset.py              # Bitcoin OHLCV with historical backfill
│   ├── btc_dataset_github_sync_dag.py        # OHLCV + 90+ indicators → GitHub dataset
│   ├── technical_indicators_dag.py           # 110+ TA-Lib indicators in Snowflake
│   ├── on_chain_trader_dag.py                # On-chain trading signals
│   ├── financial_market_data_dag.py          # NASDAQ, VIX, DXY, Gold
│   ├── funding_rate_updater.py               # BTC funding rates (8-hourly)
│   ├── OpenInterestFutures_data_updater_dag.py  # BTC open interest (multi-exchange)
│   ├── bitcoin_news_dag.py                   # Guardian + Finnhub + AlphaVantage news
│   ├── snowflake_docs_db_dag.py              # Snowflake docs → Pinecone RAG
│   ├── aws_saa_docs_scraper.py               # AWS SAA docs → Pinecone RAG
│   ├── iics_dbt_doc_scraper.py               # dbt + IICS docs → Pinecone RAG
│   └── snowflake_migrate_dag.py              # Snowflake trial account migration
├── config/
│   └── airflow.cfg
├── docker/
│   └── data/
│       ├── airflow/                          # Airflow logs and metadata
│       └── postgres/                         # PostgreSQL database
├── plugins/
├── .github/workflows/
│   └── deploy_airflow.yml                    # CI/CD pipeline
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Prerequisites

### Local Development Machine
- Docker 20.10+ and Docker Compose 2.0+
- Git
- GitHub account with repository access
- 8GB+ RAM recommended for local testing

### Remote Server (Raspberry Pi Recommended)

**Hardware:**
- Raspberry Pi >=4 (8GB+ RAM recommended) or any Linux server
- 32GB+ SD card or SSD (SSD highly recommended for performance)
- Stable internet connection

**Software:**
- Ubuntu 20.04+ or Raspberry Pi OS (64-bit)
- Docker and Docker Compose installed
- SSH access enabled

### External Services

- **Snowflake Account**: Primary data warehouse (free trial available)
- **Cloudflare Tunnel**: Secure remote access (free)
- **Telegram Bot**: Failure notifications (free)
- **GitHub**: CI/CD automation + dataset publishing (free)
- **OpenAI API**: Embeddings for RAG scrapers (pay-as-you-go)
- **Pinecone**: Vector database for RAG (free tier available)
- **Guardian API**: Bitcoin news backfill (free, 500 req/day)
- **Finnhub API**: Crypto news delta (free, 60 calls/min)
- **AlphaVantage API**: News sentiment (free, 25 req/day per key)

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/airflow-self-hosted.git
cd airflow-self-hosted
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
# Airflow
AIRFLOW_UID=1000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_secure_password_here

# Snowflake
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# Notifications
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# News APIs (for bitcoin_news_dag)
GUARDIAN_API_KEY=your_guardian_key
FINNHUB_API_KEY=your_finnhub_key
ALPHAVANTAGE_API_KEYS=["KEY1","KEY2","KEY3"]

# RAG / Embeddings (for doc scrapers)
OPENAI_API_KEY=your_openai_api_key
PINECONE_API_KEY=your_pinecone_api_key
PINECONE_SF_INDEX_NAME=your_snowflake_docs_index
PINECONE_AWS_INDEX_NAME=your_aws_docs_index

# GitHub dataset publishing (for btc_dataset_github_sync_dag)
GITHUB_USERNAME=your_github_username
GITHUB_TOKEN=your_github_pat
GITHUB_REPO_BTC_HOURLY=bitcoin-technical-indicators-dataset
```

### 3. Start Airflow Locally

```bash
# Build and start containers
docker compose up -d --build

# Wait for initialization (2-3 minutes)
docker compose logs -f airflow-init

# Access Airflow UI at http://localhost:8080
# Check status
docker compose ps
```

### 4. Add Your Own DAGs

Create a new DAG in the `dags/` directory — it will appear in the UI automatically within seconds.

---

## Production Deployment

### Automated Deployment (Recommended)

Set up GitHub Actions for automated deployment to your Raspberry Pi.

**Required GitHub Secrets:**
```
AIRFLOW_UID
_AIRFLOW_WWW_USER_USERNAME
_AIRFLOW_WWW_USER_PASSWORD
RASPBERRY_PI_USER
RASPBERRY_PI_PASSWORD
CF_ACCESS_CLIENT_ID
CF_ACCESS_CLIENT_SECRET
AIRFLOW_PATH
TOKEN
```

The CI/CD pipeline:
1. **Test Job** (every push to main): Builds image, validates DAG syntax, runs health checks
2. **Deploy Job** (after successful test): Connects via Cloudflare Tunnel, transfers files, restarts containers, validates deployment

### Manual Deployment

```bash
# On your Raspberry Pi
git clone https://github.com/yourusername/airflow-self-hosted.git
cd airflow-self-hosted
nano .env   # add your credentials
docker compose up -d --build

# Updates
git pull origin main
docker compose down
docker compose up -d --build
```

---

## Infrastructure Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Airflow Self-Hosted                        │
│                   (Raspberry Pi / Linux)                      │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │  Webserver      │  │  Scheduler      │  │  PostgreSQL │  │
│  │  (Port 8080)    │  │  (DAG Runner)   │  │  (Metadata) │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐│
│  │                 DAGs (13 pipelines)                       ││
│  │  On-chain • OHLCV • Indicators • News • Market           ││
│  │  Funding Rates • Open Interest • Doc Scrapers • Utils    ││
│  └──────────────────────────────────────────────────────────┘│
└──────────────────────────┬───────────────────────────────────┘
                           │ Data Flows
                           ▼
       ┌────────────────────────────────────────────┐
       │  External Services                          │
       │  • Snowflake (primary data warehouse)       │
       │  • GitHub (dataset publishing)              │
       │  • Pinecone (vector search / RAG)           │
       │  • bitcoin-data.com, Binance, Finnhub       │
       │  • Guardian, AlphaVantage, yfinance         │
       │  • OpenAI (embeddings)                      │
       └────────────────────────────────────────────┘
```

### Resource Configuration (Raspberry Pi Optimized)

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__PARALLELISM: 8
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 4
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: 3
AIRFLOW__API__WORKERS: 2
```

---

## Security Best Practices

- Use environment variables for all credentials; never commit `.env`
- Store secrets in GitHub Secrets for CI/CD
- Use Cloudflare Tunnel for SSH (no exposed ports)
- Enable RBAC in Airflow
- Use Fernet key encryption for Airflow connections
- Rotate passwords every 90 days

---

## Monitoring & Maintenance

```bash
# Container health
docker compose ps
docker stats

# Disk / memory
df -h && free -h

# Logs
docker compose logs --tail=100 airflow-scheduler

# Backup metadata DB
docker compose exec postgres pg_dump -U airflow > backup.sql

# Clean old logs (>30 days)
find docker/data/airflow/logs -mtime +30 -delete
```

---

## Troubleshooting

**DAG not appearing in UI:**
```bash
docker compose exec airflow-webserver airflow dags list-import-errors
docker compose exec airflow-webserver python /opt/airflow/dags/your_dag.py
```

**Out of memory on Raspberry Pi:**
```bash
# Reduce in docker-compose.yml
AIRFLOW__CORE__PARALLELISM: 4
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 2
docker compose down && docker compose up -d
```

**Connection to external service failed:**
```bash
docker compose exec airflow-webserver airflow connections test your_conn_id
```

**Disk space full:**
```bash
docker system prune -a
find docker/data/airflow/logs -mtime +30 -delete
```

---

## Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Snowflake Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Cloudflare Tunnel Documentation](https://developers.cloudflare.com/cloudflare-one/connections/connect-applications/)

---

**Status**: Production Ready  
**Last Updated**: May 2026  
**Version**: 1.2.0
