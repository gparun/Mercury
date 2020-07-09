#!/bin/bash

PYTHON_VERSION="python3.8"


MERCURY_BUCKET='mercury-bucket'
MERCURY_STACK_NAME='mercury-stack'
MERCURY_RESOURCE_OWNER='mercury-owner'

while [[ "$#" -gt 0 ]]; do
  case $1 in
  -b | --bucket)
    MERCURY_BUCKET="$2"
    ;;
  -s | --stackname)
    MERCURY_STACK_NAME="$2"
    ;;
  -e | --email)
    MERCURY_RESOURCE_OWNER="$2"
    ;;
  -p | --apitoken)
    MERCURY_API_TOKEN="$2"
    ;;
  -t | --apitesttoken)
    MERCURY_API_TEST_TOKEN="$2"
    ;;
  esac
  shift
done

echo "MERCURY BUCKET: $MERCURY_BUCKET"
echo "MERCURY STACK NAME: $MERCURY_STACK_NAME"
echo "MERCURY RESOUCE OWNER: $MERCURY_RESOURCE_OWNER"

{
  [ -z "$MERCURY_BUCKET" ] || [ -z "$MERCURY_STACK_NAME" ] || [ -z "$MERCURY_RESOURCE_OWNER" ] || [ -z "$MERCURY_API_TOKEN" ] || [ -z "$MERCURY_API_TEST_TOKEN" ]
} && {
  echo "You must provide s3 bucket, stack name, your email, prod and test API tokens"
  exit 1
}

MERCURY_SITE_PACKAGES=_dependencies/mercury/python/lib/$PYTHON_VERSION/site-packages
MERCURY_LAYER=layers/mercury
{
  echo "sam build"
  sam build
} && {
  echo "clean dependencies"
  rm -rf _dependencies/*
} && {
  echo "=============create mercury layer==========="
  mkdir -p "$MERCURY_SITE_PACKAGES/layers" &&
  cp -R "$MERCURY_LAYER" "$MERCURY_SITE_PACKAGES/layers"
} && {
  {
    echo "install mercury layer pip dependencies"
    pip install -r "$MERCURY_SITE_PACKAGES/$MERCURY_LAYER/requirements.txt" -t "$MERCURY_SITE_PACKAGES" &&
    pip freeze --path "$MERCURY_SITE_PACKAGES" >"$MERCURY_SITE_PACKAGES/$MERCURY_LAYER/installed_requirements.txt" &&
    echo "layers installed"
  } || {
    echo "failed to install pip dependencies for mercury layer"
    rm -rf _dependencies/*
    exit 1
  }
} && {
  echo "sam package"
  sam package --output-template-file packaged-template.yaml --s3-bucket "$MERCURY_BUCKET"
} && {
  echo "sam deploy"
  sam deploy --template-file packaged-template.yaml --stack-name "$MERCURY_STACK_NAME" --parameter-overrides=ResourceOwner="$MERCURY_RESOURCE_OWNER" ApiToken="$MERCURY_API_TOKEN" ApiTestToken="$MERCURY_API_TEST_TOKEN" --capabilities CAPABILITY_NAMED_IAM
}
