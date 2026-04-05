.PHONY: setup web web-debug dry-run send fast-test check-env logs clean docker-up docker-down

PYTHON := $(shell test -d .venv && echo .venv/bin/python || echo python3)
PIP    := $(shell test -d .venv && echo .venv/bin/pip || echo pip3)

# ── Setup ──────────────────────────────────────────────
setup:
	@echo "==> Creating virtualenv..."
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@mkdir -p downloads reports/prompts reports/runtime reports/exports data logs
	@test -f .env || cp .env.example .env && echo "==> .env created from .env.example"
	@echo "==> Setup complete. Run: make web"

# ── Local Web UI ───────────────────────────────────────
web:
	$(PYTHON) scripts/run_local_web.py

web-debug:
	$(PYTHON) scripts/run_local_web.py --debug

web-scheduler:
	$(PYTHON) scripts/run_local_web.py --with-scheduler

# ── CLI Pipeline ───────────────────────────────────────
dry-run:
	$(PYTHON) scripts/run_local_pipeline_test.py

send:
	$(PYTHON) scripts/run_local_pipeline_test.py --send

fast-test:
	$(PYTHON) scripts/run_local_pipeline_test.py --news-days 1 --news-max-articles 3 --transaction-limit 2

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
