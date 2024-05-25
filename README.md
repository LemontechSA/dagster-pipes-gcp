# dagster-pipes-gcp

🚧 Under construction

Deploy a simple GCP function and orchestrate it with Dagster.

## Devcontainer

This repository contains a devcontainer. I recommend that you use it.

## Setup

Add a service account JSON credentials called 'sa.json' file to '.devcontainer/.secrets'. This service account should have permissions to invoke a cloud function & read logs, but to be honest I've used a service account with very broad permissions for now.

In .devcontainer/.env, change the GOOGLE_PROJECT_ID environment variable to your GCP project id.

Open the project in the devcontainer and run `just setup`/`just s`. This will install the dependencies and configure gcloud.

## Development

In one terminal, serve the cloud function locally using the `functions-framework` library.

```
just sf
```

In another terminal, start the dagster locally:

```
just dd
```
