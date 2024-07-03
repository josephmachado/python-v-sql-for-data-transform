####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks
pytest:
	python -m pytest -p no:warnings -v ./tests

format:
	python -m black -S --line-length 79 .

isort:
	isort . --skip env

type:
	mypy --ignore-missing-imports ./src ./tests

lint:
	flake8 ./src ./tests

ci: isort format type lint pytest
