SHELL := /bin/bash
.DEFAULT_GOAL := help

# Host port for the API (change if 8000 is taken: `make up API_PORT=8001`).
API_PORT ?= 8000
export API_PORT
COMPOSE := docker compose
UV := uv run
PERF_DB_URL := postgresql+psycopg://freightpulse:freightpulse@localhost:5433/freightpulse_perf

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- local dev -----------------------------------------------------------------------------
.PHONY: install
install: ## Install Python deps into .venv (uv) and the pre-commit hooks
	uv sync
	-$(UV) pre-commit install

.PHONY: lint
lint: ## ruff lint + format check
	$(UV) ruff check .
	$(UV) ruff format --check .

.PHONY: fmt
fmt: ## Auto-format and fix lint
	$(UV) ruff format .
	$(UV) ruff check --fix .

.PHONY: typecheck
typecheck: ## mypy --strict on src/
	$(UV) mypy src

.PHONY: test
test: ## All tests (unit + integration + API; needs Docker for testcontainers)
	$(UV) pytest -q

.PHONY: test-unit
test-unit: ## Unit tests only (no Docker)
	$(UV) pytest tests/unit -q

.PHONY: test-integration
test-integration: ## Integration + API tests (testcontainers)
	$(UV) pytest tests/integration tests/api -q

.PHONY: cov
cov: ## Full test suite with coverage; gate domain+processing at 80%
	$(UV) pytest -q --cov=freightpulse --cov-report=term
	$(UV) coverage report --include="src/freightpulse/domain/*,src/freightpulse/processing/*" --fail-under=80

.PHONY: check
check: lint typecheck test ## Lint, type-check and test

# --- stack ---------------------------------------------------------------------------------
.PHONY: up
up: ## Build and start the full stack (Redpanda, Postgres, processor, sink, jobs, API, Prometheus, Grafana)
	$(COMPOSE) up -d --build
	@echo "waiting for the API to become ready..."
	@for i in $$(seq 1 90); do \
		if curl -fsS http://localhost:$(API_PORT)/ready >/dev/null 2>&1; then echo "API ready."; break; fi; \
		if [ $$i -eq 90 ]; then echo "API did not become ready"; $(COMPOSE) ps; exit 1; fi; \
		sleep 2; \
	done
	@echo ""
	@echo "  API docs        http://localhost:$(API_PORT)/docs"
	@echo "  Live alerts     http://localhost:$(API_PORT)/"
	@echo "  Redpanda UI     http://localhost:8080"
	@echo "  Grafana         http://localhost:3000  (anonymous viewer; admin/admin)"
	@echo "  Prometheus      http://localhost:9090"

.PHONY: down
down: ## Stop the stack and DELETE its volumes (clean state)
	$(COMPOSE) --profile demo down -v --remove-orphans

.PHONY: stop
stop: ## Stop the stack but keep data volumes
	$(COMPOSE) --profile demo stop

.PHONY: ps
ps: ## Show stack status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs of the app services
	$(COMPOSE) logs -f --tail=100 processor alert-sink jobs api

.PHONY: demo
demo: ## Run the fleet simulator (500 shipments, 60x) and print live fleet status + alerts
	$(COMPOSE) --profile demo run --rm demo

.PHONY: sim
sim: ## Run the simulator from the host (pass ARGS="--shipments 100 --duration 60")
	$(UV) python -m freightpulse.simulator $(ARGS)

.PHONY: dlq-replay
dlq-replay: ## Re-process every event currently in freight.dlq (after a fix)
	$(COMPOSE) run --rm --no-deps processor python -m freightpulse.processing.dlq_replay

.PHONY: migrate
migrate: ## Run Alembic migrations + partition maintenance against the compose DB (from host)
	$(UV) alembic upgrade head
	$(UV) python -m freightpulse.storage.partitions

# --- performance ---------------------------------------------------------------------------
.PHONY: seed-large
seed-large: ## Load >=5M GPS pings + >=1M events via COPY into the freightpulse_perf database
	$(UV) python -m freightpulse.storage.seed_large --database-url "$(PERF_DB_URL)"

.PHONY: sql-perf
sql-perf: ## Run the EXPLAIN (ANALYZE, BUFFERS) before/after case studies (needs seed-large)
	$(UV) python -m freightpulse.perf.sql_cases --database-url "$(PERF_DB_URL)"

.PHONY: bench
bench: ## Throughput/latency benchmark (1 and 3 processors); writes docs/benchmarks.md
	$(UV) python -m freightpulse.perf.bench

.PHONY: clean
clean: ## Remove caches
	rm -rf .mypy_cache .ruff_cache .pytest_cache .hypothesis htmlcov .coverage coverage.xml
