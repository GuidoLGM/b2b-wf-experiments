## Set-up

Create a bucket using the following name pattern:

```
gs://pulumi-state-$PROJECT_ID-$STACK_NAME
```

The final result should be something like:

```
gs://pulumi-state-wb-ai-acltr-tbs-3-pr-a62583-sample
```

## How to use

``` shell
$ gcloud auth application-default login --no-launch-browser
$ gcloud config set project wb-ai-acltr-tbs-3-pr-a62583
```

### Linux
``` sh
$HOME/.config/gcloud/application_default_credentials.json
```

### Windows
``` powershell
%APPDATA%\gcloud\application_default_credentials.json
```

### Running

First, build you image

```
docker build . --tag pulumi-deploy:dev
```

Then, run it using your settings

```
docker run --rm 
    -v $HOME/.config/gcloud/application_default_credentials.json:/secrets/application_default_credentials.json 
    -v your-passphrase-file:/secrets/passphrase.txt
    -v your-bi-layer-stacks-folder:/stacks
    -e PROXY_ADDRESS=198.161.14.25 
    -e PROXY_PORT=8080
    -e STACK_NAME=sample
    -e PROJECT_ID=wb-ai-acltr-tbs-3-pr-a62583
    -e PROJECT_TYPE=wb
    pulumi-deploy:dev
```