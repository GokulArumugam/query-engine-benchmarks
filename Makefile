PYTHON ?= python3
ENGINES ?= duckdb,polars,spark
SF ?= 0.1,1
comma := ,
SF_ARGS := $(subst $(comma), ,$(SF))

.PHONY: data bench test

data:
	$(PYTHON) -m bench.datagen --sf $(SF_ARGS)

bench: data
	$(PYTHON) -m bench.runner --engine $(ENGINES) --sf $(SF)
	$(PYTHON) -m results.summarize

test:
	$(PYTHON) -m pytest -q
