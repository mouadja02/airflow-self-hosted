FROM apache/airflow:3.0.1

# Switch to root user to install packages
USER root

# Install system dependencies including git
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --quiet -r /tmp/requirements.txt