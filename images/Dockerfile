FROM gcr.io/deeplearning-platform-release/base-cpu:latest

RUN apt-get update && \ 
    apt-get install -y --no-install-recommends \
        gcc \ 
        g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade pip setuptools wheel

RUN pip install \
        scikit-learn==1.5.2 \
        pandas==2.2.3 \
        google-cloud-bigquery==3.26.0 \
        google-cloud-bigquery-storage==2.27.0 \
        db-dtypes==1.3.0 \
        ipykernel==6.29.0 \
        google-cloud-workflows==1.15.1 \
        google-cloud-aiplatform==1.74.0 \
        kfp==2.11.0