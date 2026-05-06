# Research_Project — convenience targets (optional). From repo root.
# Windows: install Make (e.g. chocolatey make) or run the python commands from README.

PYTHON ?= python

.PHONY: help full refresh reports rebuild format install

help:
	@echo Full scrape + exports (slow):     make full   OR  $(PYTHON) scripts/run_pipeline.py
	@echo Rebuild workbook from CSV (fast): make reports OR $(PYTHON) scripts/build_workbook_only.py
	@echo Excel formatting only (fast):    make format OR $(PYTHON) scripts/reformat_workbook.py

full refresh:
	$(PYTHON) scripts/run_pipeline.py

reports rebuild:
	$(PYTHON) scripts/build_workbook_only.py

format:
	$(PYTHON) scripts/reformat_workbook.py

install:
	$(PYTHON) -m pip install -r requirements.txt
