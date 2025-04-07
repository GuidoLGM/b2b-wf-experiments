FROM python:3.11.11-slim AS base

RUN apt-get clean && rm -rf /var/lib/apt/lists/* 

RUN apt-get update && apt-get install -y \
        curl \
        apt-transport-https \
        ca-certificates \
        gnupg

RUN  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-cli -y
RUN  curl -fsSL https://get.pulumi.com | sh 

RUN  pip install --upgrade \
    pulumi-policy==1.4.0 \
    yq==3.2.2 \
    protobuf==4.21.1 \
    j2cli==0.3.10

ENV PATH=${PATH}:/root/.pulumi/bin

RUN   pulumi plugin install resource gcp  8.7.0 \
    ; pulumi plugin install resource google-native 0.32.0 \
    ; pulumi plugin install resource str 1.0.0 \
    ; pulumi plugin install resource random 4.16.0

COPY pulumi-build.sh /bin/

COPY stacks /stacks
COPY secrets /secrets
COPY build /build

CMD [ "/bin/pulumi-build.sh" ]
