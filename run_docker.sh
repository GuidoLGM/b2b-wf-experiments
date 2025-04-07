docker run --rm \
    -v $HOME/.config/gcloud/application_default_credentials.json:/secrets/application_default_credentials.json \
    -v ./secrets/passphrase.txt:/secrets/passphrase.txt \
    -v ./stacks:/stacks \
    -e PROXY_ADDRESS=198.161.14.25 \
    -e PROXY_PORT=8080 \
    -e STACK_NAME=workforce \
    -e PROJECT_ID=wb-ai-acltr-tbs-3-pr-a62583 \
    -e PROJECT_TYPE=wb \
    pulumi-deploy:dev