ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

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
# Keep dependency resolution from silently replacing the selected Airflow version.
RUN pip install --no-cache-dir --quiet "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
    && pip check

# Keep operational changes independent of the dependency installation layer.
COPY scripts/check_file_descriptors.py /opt/airflow/scripts/check_file_descriptors.py
COPY scripts/patch_airflow_logging.py /opt/airflow/scripts/patch_airflow_logging.py
COPY config/airflow_local_settings.py /opt/airflow/custom_config/airflow_local_settings.py
RUN python /opt/airflow/scripts/patch_airflow_logging.py
