.PHONY: install lint test type precommit

install:
	poetry install

lint:
	poetry run ruff check .
	poetry run black --check .
	poetry run isort --check-only .

type:
	poetry run mypy src

test:
	poetry run pytest -q

precommit:
	pre-commit install
