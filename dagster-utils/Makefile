.PHONY: deps black isort test coverage

deps:  ## Install dependencies
	poetry install

black:  ## Lint and static-check
	black .

isort:
	isort . --profile black

test:  ## Run tests
	poetry run pytest -vv

coverage:  ## Run tests with coverage
	pytest --cov
