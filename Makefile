SHELL := /bin/bash

.PHONY: help dev api frontend health test test-templates test-ui test-stress smoke-prod triage-csvs

help:
	@echo "Loom developer commands"
	@echo "  make dev       Start FastAPI + Vite together"
	@echo "  make api       Start the FastAPI backend only"
	@echo "  make frontend  Start the Vite frontend only"
	@echo "  make health    Check local frontend and API health"
	@echo "  make test      Run the Python test suite"
	@echo "  make test-templates Run template contract and stress-matrix tests"
	@echo "  make test-ui   Run Playwright browser tests"
	@echo "  make test-stress Run backend, browser, and hosted smoke checks"
	@echo "  make smoke-prod Run hosted Netlify + Render smoke checks"
	@echo "  make triage-csvs ROOT=/path Run detect + build triage over CSV files"

dev:
	@./scripts/dev.sh

api:
	@source .venv/bin/activate && uvicorn pipeline.api.app:app --reload --port 8000

frontend:
	@cd frontend && npm run dev -- --host 127.0.0.1 --port 5173

health:
	@./scripts/healthcheck.sh

test:
	@python3 -m unittest discover -s tests

test-templates:
	@python3 -m unittest discover -s tests -p 'test_template_contracts.py'
	@python3 -m unittest discover -s tests -p 'test_stress_matrix.py'

test-ui:
	@cd frontend && npm run test:e2e

test-stress:
	@$(MAKE) test
	@$(MAKE) test-ui
	@$(MAKE) smoke-prod

smoke-prod:
	@python3 scripts/smoke_prod.py

triage-csvs:
	@python3 scripts/triage_csvs.py $(ROOT)
