#! /bin/sh

# Formatting
# ------------------------------------------------------------------------------------------------------------
PURPLE='\033[0;35m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
SET='\033[0m'

echo $PURPLE '
_______________   _______                 ______                  _____              
___    |___  _/   ___    |___________________  /_________________ __  /______________
__  /| |__  /     __  /| |  ___/  ___/  _ \_  /_  _ \_  ___/  __ `/  __/  __ \_  ___/
_  ___ |_/ /      _  ___ / /__ / /__ /  __/  / /  __/  /   / /_/ // /_ / /_/ /  /    
/_/  |_/___/      /_/  |_\___/ \___/ \___//_/  \___//_/    \__,_/ \__/ \____//_/     
                                                                                     
' $SET
# ------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------
# Proxy Settings
# ------------------------------------------------------------------------------------------------------------
if [ -z $PROXY_ADDRESS ] || [ -z $PROXY_PORT ];
then
    echo "$YELLOW> Proxy address or port is not set.$SET" 
    echo "$YELLOW> Please set the proxy address and port in the environment variables.$SET"
    echo "$YELLOW> Proceeding without proxy.$SET"
    echo "$YELLOW> For TELUS PROXY use the following values: \n\n\tPROXY_ADDRESS=198.161.14.25\n\tPROXY_PORT=8080$SET\n"
else
    echo "$GREEN> Setting Proxy using http://$PROXY_ADDRESS:$PROXY_PORT $SET"
    export HTTP_PROXY=http://$PROXY_ADDRESS:$PROXY_PORT
    export HTTPS_PROXY=http://$PROXY_ADDRESS:$PROXY_PORT
    gcloud config set proxy/type http
    gcloud config set proxy/address $PROXY_ADDRESS
    gcloud config set proxy/port $PROXY_PORT
fi
# ------------------------------------------------------------------------------------------------------------

PROJECT_TYPE="wb"
if [ -z $PROJECT_ID ] || [ -z $STACK_NAME ];
then
    echo "$YELLOW> Project ID and Stack Name are not set.$SET" 
    echo "$YELLOW> Using default values for sample.\n\n\tPROJECT_ID=wb-ai-acltr-tbs-3-pr-a62583\n\tSTACK_NAME=sample$SET" 

    PROJECT_ID=wb-ai-acltr-tbs-3-pr-a62583
    STACK_NAME=sample
fi

# ------------------------------------------------------------------------------------------------------------
# Google Cloud Settings
# ------------------------------------------------------------------------------------------------------------
echo "$GREEN> Authenticating User on Google Cloud $SET"

export GOOGLE_APPLICATION_CREDENTIALS="/secrets/application_default_credentials.json"

# If the user generated the credentials file, this should not be necessary
gcloud config set project $PROJECT_ID
# ------------------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------------------------------------------------
# Pulumi
# ------------------------------------------------------------------------------------------------------------
cd /stacks/$STACK_NAME
cp -r /stacks/$STACK_NAME /build/$STACK_NAME

echo "$GREEN> Running Pulumi commands for $(pwd) $SET"
export PULUMI_CONFIG_PASSPHRASE_FILE=/secrets/passphrase.txt

if [ -e "./Pulumi.yaml" ]; then
    j2 --import-env VAR Pulumi.yaml -o /build/$STACK_NAME/Pulumi.yaml
    cd /build/$STACK_NAME
    pulumi --non-interactive login --cloud-url gs://pulumi-state-$PROJECT_ID-$STACK_NAME
    pulumi --non-interactive stack select $STACK_NAME --create
    pulumi --non-interactive config set project $PROJECT_ID
    pulumi --non-interactive config set gcp:project $PROJECT_ID
    pulumi --non-interactive config set builder "self-hosted"
    pulumi --non-interactive config set project_sha "self-hosted"
    pulumi --color always --emoji up --yes
    cd /stacks/$STACK_NAME
fi 

for inner_stack in *; do
    if [ -d "$inner_stack" ]; then
        cd $inner_stack
        echo "> Processing directory: $inner_stack"
        if [ -e "./Pulumi.yaml" ]; then
            j2 --import-env VAR Pulumi.yaml -o /build/$STACK_NAME/$inner_stack/Pulumi.yaml
            cd /build/$STACK_NAME/$inner_stack
            pulumi --non-interactive login --cloud-url gs://pulumi-state-$PROJECT_ID-$STACK_NAME/$inner_stack
            pulumi --non-interactive stack select $STACK_NAME --create
            pulumi --non-interactive config set project $PROJECT_ID
            pulumi --non-interactive config set gcp:project $PROJECT_ID
            pulumi --non-interactive config set builder "self-hosted"
            pulumi --non-interactive config set project_sha "self-hosted"
            pulumi --color always --emoji up --yes
        fi
        cd /stacks/$STACK_NAME
    fi
done
# ------------------------------------------------------------------------------------------------------------