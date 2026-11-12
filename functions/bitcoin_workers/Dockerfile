FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y git tar gzip gcc-c++ make \
    && curl --fail --location --retry 3 https://github.com/git-lfs/git-lfs/releases/download/v3.7.0/git-lfs-linux-amd64-v3.7.0.tar.gz -o /tmp/git-lfs.tar.gz \
    && mkdir /tmp/git-lfs \
    && tar -xzf /tmp/git-lfs.tar.gz -C /tmp/git-lfs --strip-components=1 \
    && cp /tmp/git-lfs/git-lfs /usr/local/bin/git-lfs \
    && rm -rf /tmp/git-lfs /tmp/git-lfs.tar.gz \
    && dnf clean all

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && pip check

COPY --chmod=644 worker.py bitcoin_indicators.py ${LAMBDA_TASK_ROOT}/
COPY --chmod=644 test_worker.py ${LAMBDA_TASK_ROOT}/
CMD ["worker.handler"]
