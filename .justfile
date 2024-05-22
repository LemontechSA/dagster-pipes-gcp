alias s := setup
alias p := pre_commit

install:
  poetry install --with dev

pre_commit_setup:
  poetry run pre-commit install

setup: install pre_commit_setup

pre_commit:
  poetry run pre-commit run -a
