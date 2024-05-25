set dotenv-path := "/home/vscode/workspace/.devcontainer/.env"

alias s := setup
alias p := pre_commit
alias dd := dagster_dev
alias s := serve_cloud_function


set_project:
  @echo "Setting gcloud project to $GOOGLE_PROJECT_ID"
  gcloud config set project $GOOGLE_PROJECT_ID

auth:
  gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

serve_cloud_function:
  poetry run functions-framework --target=main --source=src/main.py --debug

call:
  curl -m 70 -X POST http://127.0.0.1:8080 \
    -H "Content-Type: application/json" \
    -H "X-Cloud-Trace-Context: 00aa1122334455/043" \
    -d '{"some_parameter_value": 1}'

dagster_dev:
  #!/usr/bin/env bash
  mkdir -p .dagster
  export DAGSTER_HOME=/home/vscode/workspace/.dagster
  cd dagster && poetry run dagster dev -f DAG.py

install:
  poetry install --with dev --with local

pre_commit_setup:
  poetry run pre-commit install

setup: install pre_commit_setup auth set_project

pre_commit:
  poetry run pre-commit run -a
