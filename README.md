# 🚀 Airflow Self-Hosted Infrastructure

A production-ready Apache Airflow deployment system for personal data engineering projects, optimized for Raspberry Pi and self-hosted environments with automated CI/CD pipeline.

## 📋 Overview

This project provides a **complete self-hosted Airflow infrastructure** that can be deployed on Raspberry Pi or any Linux server. It serves as the backbone for multiple data engineering projects, providing reliable data pipelines, scheduling, and orchestration capabilities.

### Why Self-Hosted Airflow?

- **Cost-Effective**: Run on Raspberry Pi (~$100) instead of cloud services ($100+/month)
- **Full Control**: Complete control over your data pipeline infrastructure
- **Privacy**: Keep your data and workflows on your own hardware
- **Learning**: Perfect for personal projects and learning data engineering
- **Scalable**: Start small, scale as needed with additional workers
- **Always On**: 24/7 data pipeline execution without cloud bills

### Key Features

- **Production-Ready**: Battle-tested deployment with error handling and monitoring
- **Automated CI/CD**: GitHub Actions pipeline for testing and deployment
- **Docker-Based**: Containerized deployment with Docker Compose
- **ARM-Optimized**: Performance tuned for Raspberry Pi 5
- **Secure Remote Access**: Cloudflare Tunnel integration for secure SSH
- **Multi-Project Support**: Serves multiple downstream data projects
- **Snowflake Integration**: Native support for Snowflake data warehouse
- **Extensible**: Easy to add new DAGs and integrations

---

## 🎯 Use Cases

This Airflow infrastructure serves various personal data engineering projects:

### Current Projects

1. **Trading Agent Data Pipeline** (Primary)
   - Fetches Bitcoin hourly OHLCV data from CryptoCompare API
   - Historical backfill from 2010 with intelligent batching
   - Calculates 110+ technical indicators using TA-Lib
   - Stores data in Snowflake for ML model training
   - Provides fresh data for algorithmic trading strategies

2. **Snowflake Documentation Scraper** (Knowledge Base)
   - Scrapes Snowflake documentation for SnowPro Core exam prep
   - Delta-mode scraping (only new/updated pages)
   - Parallel web scraping with rate limiting
   - Vector embeddings with OpenAI for semantic search
   - Pinecone vector database for RAG applications

3. **AWS SAA Documentation Scraper** (Knowledge Base)
   - Scrapes AWS SAA documentation for SnowPro Core exam prep
   - Delta-mode scraping (only new/updated pages)
   - Parallel web scraping with rate limiting
   - Vector embeddings with OpenAI for semantic search
   - Pinecone vector database for RAG applications

4. **Future Projects** (Examples)
   - ETL pipelines for personal analytics
   - Social media data aggregation
   - IoT sensor data processing
   - Automated reporting and dashboards

### Example DAGs Included

The repository includes production-ready example DAGs:

- **`bitcoin_ohlcv_dataset.py`**: Bitcoin price data collection with historical backfill
- **`technical_indicators_dag.py`**: Technical analysis calculations
- **`snowflake_docs_db_dag.py`**: Documentation scraper with vector embeddings
- **`aws_saa_docs_scraper.py`**: AWS documentation scraper for SAA certification with vector embeddings

These DAGs demonstrate best practices and can be used as templates for your own projects.

---

## 🏗️ Project Structure

```
airflow-self-hosted/
├── dags/                             # Airflow DAGs directory
│   ├── bitcoin_ohlcv_dataset.py     # Bitcoin price data with backfill
│   ├── technical_indicators_dag.py  # Technical analysis calculations
│   ├── snowflake_docs_db_dag.py     # Documentation scraper + RAG
│   └── aws_saa_docs_scraper.py      # AWS SAA documentation scraper + RAG
│   └── your_custom_dag.py           # Add your own DAGs here
├── config/
│   └── airflow.cfg               # Airflow configuration
├── docker/
│   └── data/                     # Docker persistent volumes
│       ├── airflow/              # Airflow logs and metadata
│       └── postgres/             # PostgreSQL database
├── plugins/                      # Airflow custom plugins
├── .github/workflows/
│   └── deploy_airflow.yml       # CI/CD pipeline
├── docker-compose.yml           # Docker Compose orchestration
├── Dockerfile                   # Custom Airflow image
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## 🛠️ Prerequisites

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
- Power supply and cooling (fan or heatsink)

**Software:**
- Ubuntu 20.04+ or Raspberry Pi OS (64-bit)
- Docker and Docker Compose installed
- SSH access enabled

### External Services (Optional)

- **Snowflake Account**: For data warehousing (free trial available)
- **Cloudflare Tunnel**: For secure remote access (free)
- **Telegram Bot**: For notifications (free)
- **GitHub**: For CI/CD automation (free)
- **OpenAI API**: For embeddings generation (pay-as-you-go)
- **Pinecone**: For vector database storage (free tier available)

---

## 🔧 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/airflow-self-hosted.git
cd airflow-self-hosted
```

### 2. Configure Environment

Create a `.env` file in the project root:

```env
# Airflow Configuration
AIRFLOW_UID=1000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=your_secure_password_here

# Snowflake Configuration (if using Snowflake)
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# Telegram Notifications
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# OpenAI & Pinecone (for documentation scraper)
OPENAI_API_KEY=your_openai_api_key
PINECONE_API_KEY=your_pinecone_api_key
PINECONE_SF_INDEX_NAME=your_pinecone_sf_index_name
PINECONE_AWS_INDEX_NAME=your_pinecone_aws_index_name

```

### 3. Start Airflow Locally

```bash
# Build and start containers
docker compose up -d --build

# Wait for initialization (2-3 minutes)
docker compose logs -f airflow-init

# Access Airflow UI
# http://localhost:8080
# Username: admin
# Password: (from .env file)

# Check status
docker compose ps

# View logs
docker compose logs -f airflow-scheduler

# Stop containers
docker compose down -v
```

### 4. Add Your Own DAGs

Create a new DAG in the `dags/` directory:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def my_task():
    print("Hello from my custom DAG!")
    # Your data engineering logic here

default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'my_custom_dag',
    default_args=default_args,
    description='My custom data pipeline',
    schedule='@daily',  # or '0 * * * *' for hourly
    catchup=False,
    tags=['custom', 'my-project'],
) as dag:
    
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task
    )
```

The DAG will automatically appear in the Airflow UI within seconds!

---

## 🚀 Production Deployment

### Option 1: Automated Deployment (Recommended)

Set up GitHub Actions for automated deployment to your Raspberry Pi.

#### Configure GitHub Secrets

Go to your repository → Settings → Secrets and variables → Actions, and add:

**Required Secrets:**
```
AIRFLOW_UID                       # 1000
_AIRFLOW_WWW_USER_USERNAME        # admin
_AIRFLOW_WWW_USER_PASSWORD        # Strong password
RASPBERRY_PI_USER                 # SSH username
RASPBERRY_PI_PASSWORD             # SSH password
CF_ACCESS_CLIENT_ID               # Cloudflare Access Client ID
CF_ACCESS_CLIENT_SECRET           # Cloudflare Access Client Secret
AIRFLOW_PATH                      # /home/user/airflow-self-hosted
TOKEN                             # GitHub PAT with repo access
```

**Optional Secrets (for example DAGs):**
```
SNOWFLAKE_ACCOUNT                 # If using Snowflake
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE
TELEGRAM_BOT_TOKEN                # For notifications
TELEGRAM_CHAT_ID
```

#### Deployment Workflow

The CI/CD pipeline automatically:

1. **Test Job** (on every push to main):
   - Builds Docker image
   - Validates DAG syntax
   - Runs health checks
   - Cleans up resources

2. **Deploy Job** (after successful test):
   - Connects via Cloudflare Tunnel
   - Transfers updated files
   - Updates environment variables
   - Restarts Docker containers
   - Validates deployment

Simply push to main branch, and your Raspberry Pi will automatically update!

### Option 2: Manual Deployment

On your Raspberry Pi:

```bash
# Initial setup
cd ~
git clone https://github.com/yourusername/airflow-self-hosted.git
cd airflow-self-hosted

# Create .env file with your credentials
nano .env

# Start Airflow
docker compose up -d --build

# Check status
docker compose ps
docker compose logs -f airflow-webserver

# Access Airflow UI
# http://raspberry-pi-ip:8080
```

**Updates:**
```bash
cd ~/airflow-self-hosted
git pull origin main
docker compose down
docker compose up -d --build
```

---

## 📊 Infrastructure Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Airflow Self-Hosted                      │
│                    (Raspberry Pi / Linux)                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐│
│  │  Airflow        │  │  Airflow        │  │  PostgreSQL  ││
│  │  Webserver      │  │  Scheduler      │  │  Database    ││
│  │  (Port 8080)    │  │  (DAG Runner)   │  │  (Metadata)  ││
│  └─────────────────┘  └─────────────────┘  └──────────────┘│
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              DAGs Directory (Your Pipelines)             ││
│  │  • bitcoin_ohlcv_dataset.py                             ││
│  │  • technical_indicators_dag.py                          ││
│  │  • your_custom_dag.py                                   ││
│  └─────────────────────────────────────────────────────────┘│
│                                                               │
└───────────────────────┬───────────────────────────────────────┘
                        │
                        │ Data Flows
                        ▼
        ┌───────────────────────────────────┐
        │    External Services              │
        │  • Snowflake (Data Warehouse)     │
        │  • APIs (CryptoCompare, etc.)     │
        │  • Databases (MySQL, Postgres)    │
        │  • Cloud Storage (S3, GCS)        │
        └───────────────────────────────────┘
```

### Resource Configuration (Raspberry Pi Optimized)

```yaml
# docker-compose.yml settings
AIRFLOW__CORE__EXECUTOR: LocalExecutor           # Lightweight
AIRFLOW__CORE__PARALLELISM: 8                    # Max parallel tasks
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 4      # Tasks per DAG
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1      # One run at a time
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: 3     # DB connections
AIRFLOW__API__WORKERS: 2                         # API workers
```

These settings ensure Airflow runs smoothly on Raspberry Pi 4 with 4GB RAM.

---

## 📝 Adding Your Own Projects

### Step 1: Create Your DAG

```python
# dags/my_project_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests

def fetch_data(**context):
    """Fetch data from your API"""
    response = requests.get('https://api.example.com/data')
    data = response.json()
    context['ti'].xcom_push(key='raw_data', value=data)
    print(f"✅ Fetched {len(data)} records")

def transform_data(**context):
    """Transform your data"""
    raw_data = context['ti'].xcom_pull(key='raw_data')
    # Your transformation logic
    transformed = [process(item) for item in raw_data]
    context['ti'].xcom_push(key='transformed_data', value=transformed)
    print(f"✅ Transformed {len(transformed)} records")

def load_to_database(**context):
    """Load data to Snowflake or other DB"""
    data = context['ti'].xcom_pull(key='transformed_data')
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    # Your load logic
    print(f"✅ Loaded {len(data)} records to database")

default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'my_project_etl',
    default_args=default_args,
    description='ETL pipeline for my project',
    schedule='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['my-project', 'etl'],
) as dag:
    
    fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data
    )
    
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    load = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database
    )
    
    fetch >> transform >> load
```

### Step 2: Add Dependencies (if needed)

```bash
# Add to requirements.txt
your-library==1.0.0
another-package>=2.0.0
```

### Step 3: Deploy

```bash
# Commit and push
git add dags/my_project_dag.py requirements.txt
git commit -m "Add my project DAG"
git push origin main

# CI/CD will automatically deploy to your Raspberry Pi!
```

### Step 4: Monitor

- Access Airflow UI: `http://your-raspberry-pi:8080`
- Enable your DAG
- Trigger a test run
- Monitor logs and task status

---

## 🔒 Security Best Practices

### Credentials Management
- ✅ Use environment variables for all credentials
- ✅ Store secrets in GitHub Secrets for CI/CD
- ✅ Never commit `.env` file to repository
- ✅ Rotate passwords every 90 days
- ✅ Use strong passwords (16+ characters, mixed case, numbers, symbols)

### Network Security
- ✅ Use Cloudflare Tunnel for SSH (no exposed ports)
- ✅ Configure UFW firewall on Raspberry Pi
- ✅ Use HTTPS for Airflow web UI (via reverse proxy)
- ✅ Restrict database access by IP when possible
- ✅ Keep Raspberry Pi OS and Docker updated

### Airflow Security
- ✅ Change default admin password immediately
- ✅ Enable RBAC (Role-Based Access Control)
- ✅ Use Fernet key encryption for connections
- ✅ Regular security updates via Docker image rebuilds
- ✅ Review DAG code before deployment

### Cloudflare Tunnel Setup

```bash
# On Raspberry Pi
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb -o cloudflared.deb
sudo dpkg -i cloudflared.deb

# Authenticate
cloudflared tunnel login

# Create tunnel
cloudflared tunnel create airflow-pi

# Configure tunnel
nano ~/.cloudflared/config.yml
```

---

## 📊 Monitoring & Maintenance

### Daily Monitoring

**Airflow UI Dashboard:**
- Check DAG run status
- Review task duration trends
- Monitor failure rates
- Check scheduler heartbeat

**System Health:**
```bash
# Check Docker containers
docker compose ps

# Check resource usage
docker stats

# Check disk space
df -h

# Check memory
free -h
```

### Weekly Maintenance

```bash
# Review logs
docker compose logs --tail=100 airflow-scheduler

# Check for updates
docker compose pull

# Backup Airflow metadata
docker compose exec postgres pg_dump -U airflow > backup.sql

# Clean old logs (optional)
find docker/data/airflow/logs -mtime +30 -delete
```

### Monthly Tasks

- Review and optimize DAG performance
- Update Python dependencies
- Security audit and password rotation
- Review resource usage trends
- Plan capacity upgrades if needed

### Performance Metrics

**Key Indicators:**
- DAG run duration: < 10 minutes (typical)
- Task success rate: > 99%
- Scheduler lag: < 1 second
- Memory usage: < 80% of available
- CPU usage: < 70% average

---

## 🐛 Troubleshooting

### Common Issues

**1. DAG Not Appearing in UI**
```bash
# Check for parsing errors
docker compose exec airflow-webserver airflow dags list-import-errors

# Validate DAG syntax
docker compose exec airflow-webserver python /opt/airflow/dags/your_dag.py

# Restart scheduler
docker compose restart airflow-scheduler
```

**2. Out of Memory on Raspberry Pi**
```bash
# Check memory usage
free -h

# Reduce parallelism in docker-compose.yml
AIRFLOW__CORE__PARALLELISM: 4
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 2

# Restart services
docker compose down
docker compose up -d
```

**3. Connection to External Service Failed**
```bash
# Test connection
docker compose exec airflow-webserver airflow connections test your_conn_id

# Check environment variables
docker compose exec airflow-webserver env | grep YOUR_VAR

# Update connection in Airflow UI
# Admin → Connections → Edit
```

**4. Disk Space Full**
```bash
# Check disk usage
df -h

# Clean Docker system
docker system prune -a

# Clean old logs
find docker/data/airflow/logs -mtime +30 -delete

# Clean old DAG runs (in Airflow UI)
# Browse → DAG Runs → Delete old runs
```

**5. Scheduler Not Running**
```bash
# Check scheduler status
docker compose ps airflow-scheduler

# View scheduler logs
docker compose logs airflow-scheduler

# Restart scheduler
docker compose restart airflow-scheduler
```

### Log Locations

```bash
# Airflow logs
docker/data/airflow/logs/

# Scheduler logs
docker compose logs airflow-scheduler

# Webserver logs
docker compose logs airflow-webserver

# PostgreSQL logs
docker compose logs postgres

# Specific DAG run logs
docker/data/airflow/logs/dag_id=your_dag/run_id=*/task_id=*/
```

---

## 📚 Resources

### Documentation
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Raspberry Pi Documentation](https://www.raspberrypi.org/documentation/)
- [Cloudflare Tunnel Documentation](https://developers.cloudflare.com/cloudflare-one/connections/connect-applications/)

### Airflow Providers
- [Snowflake Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/)
- [AWS Provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/)
- [Google Cloud Provider](https://airflow.apache.org/docs/apache-airflow-providers-google/)
- [HTTP Provider](https://airflow.apache.org/docs/apache-airflow-providers-http/)
- [All Providers](https://airflow.apache.org/docs/apache-airflow-providers/)

### Community
- [Airflow Slack](https://apache-airflow.slack.com/)
- [Airflow GitHub](https://github.com/apache/airflow)
- [Stack Overflow - Airflow](https://stackoverflow.com/questions/tagged/airflow)
- [Reddit - r/dataengineering](https://www.reddit.com/r/dataengineering/)

### Learning Resources
- [Airflow Fundamentals](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag)

---

## 🎯 Example Projects Using This Infrastructure

### 1. Trading Agent Data Pipeline

**Purpose**: Provide fresh market data for algorithmic trading

**DAGs:**
- `bitcoin_ohlcv_dataset.py`: Fetches Bitcoin OHLCV data with historical backfill
- `technical_indicators_dag.py`: Calculates 110+ technical indicators

**Features:**
- Intelligent historical data initialization from 2010
- Branching logic to skip backfill if data exists
- Batch processing with rate limiting
- Delta updates for recent data

**Schedule**: Daily at 00:05 UTC
**Data Destination**: Snowflake data warehouse
**Downstream Use**: ML model training, backtesting, live trading

### 2. Snowflake Documentation Knowledge Base

**Purpose**: Create searchable vector database of Snowflake documentation

**DAG:**
- `snowflake_docs_db_dag.py`: Scrapes and vectorizes documentation

**Features:**
- Delta-mode scraping (only new/changed URLs)
- Parallel web scraping with ThreadPoolExecutor
- OpenAI embeddings for semantic search
- Pinecone vector storage for RAG applications
- Airflow Variable tracking of scraped URLs

**Schedule**: Weekly on Sunday at 2 AM
**Data Destination**: Pinecone vector database
**Downstream Use**: SnowPro Core exam preparation, RAG chatbot

### 3. Your Custom Projects (Add Your Own!)

**Examples:**
- Web scraping for price monitoring
- Social media sentiment analysis
- IoT sensor data aggregation
- Personal finance tracking
- Weather data collection
- News aggregation and analysis
- Email report automation
- Database backup automation

---

## 🚀 Getting Started Checklist

- [ ] Clone repository
- [ ] Create `.env` file with credentials
- [ ] Start Airflow locally: `docker compose up -d`
- [ ] Access UI at `http://localhost:8080`
- [ ] Review example DAGs
- [ ] Create your first custom DAG
- [ ] Test DAG locally
- [ ] Configure GitHub Secrets
- [ ] Deploy to Raspberry Pi
- [ ] Set up monitoring and alerts
- [ ] Add more projects as needed!

---

## 📄 License

This project is licensed under the MIT License - feel free to use it for your personal data engineering projects!

---

## 🎉 Acknowledgments

- Apache Airflow community for excellent orchestration platform
- Raspberry Pi Foundation for affordable computing
- Docker team for containerization technology
- Cloudflare for secure tunnel solution
- Open source community for inspiration and support

---

**Project Type**: Self-Hosted Data Engineering Infrastructure  
**Primary Use**: Personal data pipeline orchestration  
**Example Projects**: Trading agent, web scraping, IoT, analytics  
**Status**: Production Ready ✅  
**Last Updated**: February 2, 2026  
**Version**: 1.1.0
