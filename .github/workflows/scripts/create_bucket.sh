#!/usr/bin/env bash

# NB: this script can be run from the CI/CD 'setup' pipeline (manual trigger).

set -e

BUCKET_NAME=$1

export BUCKET_EXISTS=$(gcloud storage buckets list --format=json --filter=$BUCKET_NAME | jq length)

if [ "$BUCKET_EXISTS" = "0" ]; then
    gcloud storage buckets create gs://$BUCKET_NAME  \
        --location EUROPE-WEST4 \
        --uniform-bucket-level-access
else
    echo "Bucket already exists!"
fi
