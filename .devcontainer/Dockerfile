FROM python:3.11-slim

WORKDIR /workspace



RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    apt-transport-https && \
    curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update && \
    apt-get install -y google-cloud-cli

COPY requirements.txt ./temp/requirements.txt
RUN python3.11 -m pip install --upgrade pip setuptools wheel && \
    python3.11 -m pip install --no-cache-dir -r ./temp/requirements.txt && \
    rm -rf /var/lib/apt/lists/* /root/.cache
