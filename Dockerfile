FROM apache/airflow:3.0.1

# Switch to root user to install packages
USER root

# Install system dependencies including git and TA-Lib C library
RUN apt-get update && apt-get install -y \
    build-essential \
    git git-lfs \
    curl \
    wget \
    && wget -q http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --build=aarch64-unknown-linux-gnu --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --quiet -r /tmp/requirements.txt
