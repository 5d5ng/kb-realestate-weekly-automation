.PHONY: setup web web-debug dry-run send fast-test test mcp-test mcp-test-modular mcp-test-run mcp-tunnel-init mcp-tunnel check-env logs clean docker-up docker-down

PYTHON := $(shell test -d .venv && echo .venv/bin/python || echo python3)
PIP    := $(shell test -d .venv && echo .venv/bin/pip || echo pip3)
HOST   ?= 127.0.0.1
PORT   ?= 5050

# ── Setup ──────────────────────────────────────────────
setup:
	@echo "==> Creating virtualenv..."
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@mkdir -p downloads reports/archive reports/prompts reports/runtime reports/exports data logs
	@test -f .env || cp .env.example .env && echo "==> .env created from .env.example"
	@echo "==> Setup complete. Run: make web"

# ── Local Web UI ───────────────────────────────────────
web:
	$(PYTHON) scripts/run_local_web.py --host $(HOST) --port $(PORT)

web-debug:
	$(PYTHON) scripts/run_local_web.py --host $(HOST) --port $(PORT) --debug

web-scheduler:
	$(PYTHON) scripts/run_local_web.py --host $(HOST) --port $(PORT) --with-scheduler

# ── CLI Pipeline ───────────────────────────────────────
dry-run:
	$(PYTHON) scripts/run_local_pipeline_test.py

send:
	$(PYTHON) scripts/run_local_pipeline_test.py --send

fast-test:
	$(PYTHON) scripts/run_local_pipeline_test.py --news-days 1 --news-max-articles 3 --transaction-limit 2

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py'

mcp-test:
	$(PYTHON) scripts/test_mcp_server.py

mcp-test-modular:
	$(PYTHON) scripts/test_modular_mcp.py

mcp-test-run:
	$(PYTHON) scripts/test_mcp_server.py --run-pipeline

mcp-tunnel-init:
	bash scripts/run_mcp_tunnel.sh --init

mcp-tunnel:
	bash scripts/run_mcp_tunnel.sh

# ── Utilities ──────────────────────────────────────────
check-env:
	$(PYTHON) scripts/check_env.py

logs:
	@tail -f logs/pipeline.log

clean:
	rm -rf downloads/ reports/ logs/ __pycache__ */__pycache__

# ── Docker ─────────────────────────────────────────────
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down
