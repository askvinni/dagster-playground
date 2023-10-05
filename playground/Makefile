.PHONY: deps black isort sqlfluff test coverage

deps:  ## Install dependencies
	poetry install

black:  ## Lint and static-check
	black .

isort:
	isort . --profile black

sqlfluff:
	sqlfluff fix . -y

test:  ## Run tests
	poetry run pytest -vv --ignore=dbt_project/dbt_packages

coverage:  ## Run tests with coverage
	pytest --cov
