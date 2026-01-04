# 🚀 Airflow Self-Hosted Deployment

A production-ready Apache Airflow deployment system with automated CI/CD pipeline for Raspberry Pi and other self-hosted environments.

## 📋 Features

- **Self-Hosted Deployment**: Deploy Airflow to Raspberry Pi or any Ubuntu/Linux server
- **Automated Testing**: Built-in CI/CD pipeline using GitHub Actions
- **Snowflake Integration**: Native support for Snowflake data warehouse connections
- **Secure Remote Access**: Cloudflare Tunnel integration for secure SSH access
- **Docker-Based**: Containerized deployment with Docker Compose
- **Optimized for ARM**: Settings configured for Raspberry Pi and ARM architectures
- **CI/CD Pipeline**: Automated build, test, and deployment workflow

## 🏗️ Project Structure

```
airflow-self-hosted/
├── dags/                           # Airflow DAGs directory
│   └── snowflake_test.py          # Example Snowflake DAG
├── config/
│   └── airflow.cfg                # Airflow configuration
├── docker/
│   └── data/                      # Docker data volumes
├── plugins/                        # Airflow plugins directory
├── .github/workflows/
│   └── deploy_airflow.yml         # CI/CD pipeline configuration
├── docker-compose.yml             # Docker Compose configuration
├── Dockerfile                     # Custom Airflow image
└── requirements.txt               # Python dependencies
```

## 🛠️ Prerequisites

### Local Machine
- Docker and Docker Compose
- Git
- GitHub account with repository access

### Remote Server (Raspberry Pi)
- Ubuntu 20.04+ or Raspberry Pi OS (64-bit)
- Docker and Docker Compose installed
- SSH access enabled
- 4GB+ RAM recommended

## 🔧 Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/airflow-self-hosted.git
cd airflow-self-hosted
```

### 2. Environment Configuration

Create a `.env` file with the following variables:

```env
# Airflow User
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 3. Local Testing

```bash
# Build and start containers
docker compose up -d --build

# Access Airflow UI
# http://localhost:8080

# View logs
docker compose logs -f airflow-webserver

# Stop containers
docker compose down -v
```

## 🚀 Deployment with CI/CD

### GitHub Secrets Configuration

Configure the following secrets in your GitHub repository:

```
AIRFLOW_UID                        # Airflow user ID (50000 recommended)
_AIRFLOW_WWW_USER_USERNAME        # Airflow web UI username
_AIRFLOW_WWW_USER_PASSWORD        # Airflow web UI password
SNOWFLAKE_ACCOUNT                 # Snowflake account identifier
SNOWFLAKE_USER                    # Snowflake username
SNOWFLAKE_PASSWORD                # Snowflake password
SNOWFLAKE_WAREHOUSE               # Snowflake warehouse name
SNOWFLAKE_DATABASE                # Snowflake database name
SNOWFLAKE_SCHEMA                  # Snowflake schema name
SNOWFLAKE_ROLE                    # Snowflake role name
RASPBERRY_PI_USER                 # SSH username for Raspberry Pi
RASPBERRY_PI_PASSWORD             # SSH password for Raspberry Pi
CF_ACCESS_CLIENT_ID               # Cloudflare Access Client ID
CF_ACCESS_CLIENT_SECRET           # Cloudflare Access Client Secret
AIRFLOW_PATH                      # Remote path to airflow project
TOKEN                             # GitHub personal access token
```

### Deployment Workflow

The GitHub Actions workflow includes two main jobs:

#### 1. Test Job (🧪 Build & Test)
- Checks out the code
- Creates environment variables
- Builds Docker image
- Starts containers
- Waits for health checks
- Stops and cleans up containers

**Triggers:**
- Push to main branch
- Changes to docker-compose.yml, Dockerfile, dags/**, or requirements.txt
- Manual workflow dispatch

#### 2. Deploy Job (🚀 Deploy to Raspberry Pi)
- Installs Cloudflared and SSH tools
- Configures SSH with Cloudflare Tunnel
- Transfers files to remote server
- Pulls latest code
- Updates .env file
- Restarts Docker containers

## 📝 DAG Development

### Adding New DAGs

1. Create a new Python file in the `dags/` directory:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello from Airflow!")

with DAG(
    'my_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world
    )
```

2. Commit and push to trigger the CI/CD pipeline
3. The DAG will be automatically deployed

### Snowflake Connection

The project includes pre-configured Snowflake support. Example DAG:

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

snowflake_task = SnowflakeOperator(
    task_id='snowflake_query',
    sql='SELECT * FROM your_table',
    snowflake_conn_id='snowflake_default'
)
```

## 🔒 Security

### Remote Access with Cloudflare Tunnel

This project uses Cloudflare Tunnel (cloudflared) for secure remote access without exposing ports directly:

1. **Configure Cloudflare Tunnel** on your Raspberry Pi
2. **Set Client ID and Secret** in GitHub secrets
3. **SSH access** is tunneled through Cloudflare's secure network

### Best Practices

- Keep `.env` file private and never commit to repository
- Use strong passwords for Airflow web UI
- Rotate Snowflake credentials regularly
- Enable GitHub branch protection rules
- Use GitHub Secrets for all sensitive data

## 🐳 Docker Configuration

### Image Customization

The `Dockerfile` builds a custom Airflow image with:
- Python dependencies from `requirements.txt`
- Custom DAG configurations
- Optimizations for ARM architecture (Raspberry Pi)

### Performance Optimization for Raspberry Pi

The docker-compose configuration includes Raspberry Pi optimizations:

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor           # Lightweight executor
AIRFLOW__CORE__PARALLELISM: 8                    # Limited parallelism
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 4      # Task limits
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: 3     # Connection pooling
AIRFLOW__API__WORKERS: 2                         # API workers
```

## 📊 Monitoring

### Health Checks

The deployment pipeline includes health checks that verify:
- Airflow Scheduler is running
- Airflow Web Server is responsive
- Database connections are healthy

### Logs

View logs for any service:

```bash
# Local
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler

# Remote (via SSH)
ssh user@raspberry-pi-ip
docker compose logs -f airflow-webserver
```

## 📚 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Snowflake Airflow Provider](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/)
- [Cloudflare Tunnel Documentation](https://developers.cloudflare.com/cloudflare-one/connections/connect-applications/)

## 📄 License

This project is licensed under the MIT License.

## ✉️ Support

For issues and questions, please open an issue on GitHub. 
