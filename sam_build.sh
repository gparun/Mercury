#!/bin/bash

PYTHON_VERSION="python3.8"

MERCURY_BUCKET=your-bucket
MERCURY_STACK_NAME=your-stack-name
MERCURY_RESOURCE_OWNER=your-email

MERCURY_SITE_PACKAGES=_dependencies/mercury/python/lib/$PYTHON_VERSION/site-packages
MERCURY_LAYER=layers/mercury

echo "sam build"
sam build

echo "clean dependencies"
rm -rf _dependencies/*

echo "=============create mercury layer==========="
mkdir -p "$MERCURY_SITE_PACKAGES/layers"
cp -R "$MERCURY_LAYER" "$MERCURY_SITE_PACKAGES/layers"

echo "install mercury layer pip dependencies"
pip install -r "$MERCURY_SITE_PACKAGES/$MERCURY_LAYER/requirements.txt" -t "$MERCURY_SITE_PACKAGES"

pip freeze --path "$MERCURY_SITE_PACKAGES" > "$MERCURY_SITE_PACKAGES/$MERCURY_LAYER/installed_requirements.txt"

echo "layers installed"

echo "sam package"
sam package --output-template-file packaged-template.yaml --s3-bucket "$MERCURY_BUCKET"

echo "sam deploy"
sam deploy --template-file packaged-template.yaml --stack-name "$MERCURY_STACK_NAME" --parameter-overrides=ResourceOwner="$MERCURY_RESOURCE_OWNER" --capabilities CAPABILITY_NAMED_IAM
