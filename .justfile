alias s := setup
alias p := pre_commit

set_project:
  gcloud config set project jasper-ginn-dagster

auth:
  gcloud auth activate-service-account --key-file .devcontainer/.secrets/sa.json

install:
  poetry install --with dev

pre_commit_setup:
  poetry run pre-commit install

setup: install pre_commit_setup auth set_project

pre_commit:
  poetry run pre-commit run -a
