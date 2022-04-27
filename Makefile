checkfiles = dbt/adapters test/ setup.py
py_warn = PYTHONDEVMODE=1

up:
	@pip install -r requirements_dev.txt --upgrade

deps: 
	@pip install -r requirements_dev.txt

style: deps
	@isort -src $(checkfiles)
	@black $(checkfiles)

check: deps
	@black --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@flake8 $(checkfiles)
	@bandit -x tests -r $(checkfiles)
	@mypy $(checkfiles)

test: deps
	$(py_warn) pytest

build:
	@python setup.py bdist_wheel

ci: check test
