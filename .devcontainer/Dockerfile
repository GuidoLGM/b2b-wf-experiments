FROM gcr.io/deeplearning-platform-release/base-cpu:latest

WORKDIR /workspace

# Install prerequisites
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    apt-transport-https

# Install Google Cloud SDK using the updated method
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update -y && \
    apt-get install google-cloud-cli -y

# Install Python packages
COPY requirements.txt ./temp/requirements.txt
RUN pip install --no-cache-dir -r ./temp/requirements.txt && \
    rm -rf /root/.cache
