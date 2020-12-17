
cat.PHONY: clean test install_mock_tpch install_mock_imdb docs

clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '.ipynb_checkpoints' -exec rm -fr {} +
	find . -name '__pycache__' -exec rm -fr {} +
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '.DS_Store' -exec rm -f {} +

test: ## run tests on every Python version with tox
	tox

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/pandas_log.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/ pandas_log
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

install_mock_imdb:
	python query_flow/datagen/create_imdb.py

install_mock_tpch:
	python query_flow/datagen/create_tpch.py