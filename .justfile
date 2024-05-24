alias s := setup
alias p := pre_commit

set_project:
  gcloud config set project jasper-ginn-dagster

auth:
  gcloud auth activate-service-account --key-file .devcontainer/.secrets/sa.json

serve:
  poetry run functions-framework --target=main --source=src/main.py --debug

call:
  curl -m 70 -X POST http://127.0.0.1:8080 \
    -H "Content-Type: application/json" \
    -H "X-Cloud-Trace-Context: 00aa1122334455/043" \
    -d '{"some_parameter_value": 1}'

dev:
  poetry run dagster dev -f dagster/DAG.py

install:
  poetry install --with dev

pre_commit_setup:
  poetry run pre-commit install

setup: install pre_commit_setup auth set_project

pre_commit:
  poetry run pre-commit run -a
