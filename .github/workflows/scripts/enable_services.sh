#!/usr/bin/env bash

set -e

echo "Enabling compute.googleapis.com"
gcloud services enable compute.googleapis.com

echo "Enabling artifactregistry.googleapis.com"
gcloud services enable artifactregistry.googleapis.com

echo "Enabling iam.googleapis.com"
gcloud services enable iam.googleapis.com

echo "Enabling run.googleapis.com"
gcloud services enable run.googleapis.com

echo "Enabling cloudfunctions.googleapis.com"
gcloud services enable cloudfunctions.googleapis.com

echo "Enabling storage.googleapis.com"
gcloud services enable storage.googleapis.com

echo "Enabling cloudbuild.googleapis.com"
gcloud services enable cloudbuild.googleapis.com
