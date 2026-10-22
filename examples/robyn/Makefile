# Convenience targets for the robyn-aethonan example.
# Vendored-port addition for the Aethonan example (see NOTICE).

VENV ?= /tmp/robyn-venv
PYTHON := $(VENV)/bin/python
MATURIN := $(VENV)/bin/maturin
AETHONAN_CHECKOUT ?= /home/johannes/src/granian

.PHONY: venv aethonan wheel install test

venv:
	$(PYTHON) -m pip install -U pip maturin pytest requests websocket-client websockets orjson jinja2 pydantic multipart multiprocess

aethonan:
	$(PYTHON) -m pip install $(AETHONAN_CHECKOUT)

wheel:
	$(MATURIN) build -i python --out dist
	$(PYTHON) -m pip install --force-reinstall dist/robyn_aethonan-*.whl
	$(PYTHON) install.py "$$($(PYTHON) -c 'import pathlib, robyn; print(next(pathlib.Path(robyn.__path__[0]).glob("robyn*.so")))')"

install: wheel

test:
	PYTHONPATH=$(CURDIR) ROBYN_TEST_PORT=$${ROBYN_TEST_PORT:-8199} PATH=$(VENV)/bin:$$PATH $(PYTHON) -m pytest integration_tests -q
