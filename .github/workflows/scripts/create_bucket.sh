#!/usr/bin/env bash

# NB: this script can be run from the CI/CD 'setup' pipeline (manual trigger).

set -e

GCP_PROJECT_NAME=$1

export BUCKET_EXISTS=$(gcloud storage buckets list --format=json --filter=cloud-functions-source-code | jq length)

if [ "$BUCKET_EXISTS" = "0" ]; then
    gcloud storage buckets create gs://$GCP_PROJECT_NAME-cloud-functions-source-code  \
        --location EUROPE-WEST4 \
        --uniform-bucket-level-access
else
    echo "Bucket already exists!"
fi
