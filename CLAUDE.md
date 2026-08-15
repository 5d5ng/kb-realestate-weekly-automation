# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
pip install -r requirements.txt

# CLI dry-run (no external sends)
python scripts/run_local_pipeline_test.py

# CLI with custom params
python scripts/run_local_pipeline_test.py --news-days 1 --news-max-articles 3 --transaction-limit 2
python scripts/run_local_pipeline_test.py --output-mode authoring_package

# CLI with actual sends (Telegram/SMS will fire)
python scripts/run_local_pipeline_test.py --send

# Local web UI (manual trigger via browser at http://127.0.0.1:5000)
python scripts/run_local_web.py

# Local web UI with APScheduler enabled
python scripts/run_local_web.py --with-scheduler

# Production (Railway)
gunicorn app:app --bind 0.0.0.0:$PORT --worker-class gthread --workers 1 --threads 8 --timeout 0

# Kakao OAuth initial setup (one-time, local only)
python scripts/kakao_auth.py
```

Validation includes standard-library unit tests, MCP smoke tests, and pipeline dry-runs.

```bash
make test
make mcp-test
make mcp-test-modular
```

## Architecture

Weekly pipeline that downloads KB real estate data, enriches it with transactions and news, generates multi-platform content via LLM, and delivers via Telegram/SMS.

### Pipeline flow (`scheduler.py` → `run_pipeline()`)

1. **`analyzer.py`** — Downloads KB weekly Excel from `api.kbland.kr`, parses sale/rent change sheets, computes consecutive-rise weeks, and produces six rise-focused sections (all 25 Seoul districts plus capital/non-capital top five, split by sale/rent)
2. **`realestate.py`** — Resolves region names to KB scope codes, queries KB API for 84㎡/59㎡ apartment transactions per region. Uses `ThreadPoolExecutor` (max 6 workers) for parallel region lookups. Caches via `valuation_db`
3. **`news.py`** — Queries Naver Search API for real estate keywords, filters by target publishers, scores by keyword priority. Resolves article URLs from Naver newspaper viewer format to standard URLs
4. **`reporter.py`** — Orchestrator that delegates to `reporters/` submodules for each platform and writes authoring artifacts
5. **`reporters/common.py`** — LLM provider routing (OpenAI/Gemini/Anthropic via raw HTTP, no SDKs), prompt assembly, URL placeholder protection to prevent LLM URL corruption, fallback text generation
6. **`reporters/authoring.py`** — Cost-free `reports/llm_package.md`, `reports/weekly_report.md`, and `reports/data_snapshot.json` generation for Claude/GPT web authoring and readable drafts
7. **`reporters/{telegram,instagram,cardnews,blog,alimtalk}.py`** — Platform-specific prompt builders and generators
8. **`sender.py`** — Telegram Bot API delivery with auto-chunking (3900 char limit), SOLAPI SMS/LMS delivery, KakaoTalk "나에게 보내기" via REST API
9. **`content_core/`** — Domain-neutral `content-package/v1` contract and artifact store
10. **`publishing/`** — Approval plans and channel publisher implementations
11. **`mcp_servers/content_package_server.py`** — Reusable content package MCP
12. **`mcp_servers/instagram_publisher_server.py`** — Approval-gated Instagram Login publisher MCP
13. **`mcp_runtime/`** — Shared MCP stdio/JSON-RPC runtime used by all MCP entrypoints

Instagram delivery must not be added back to the KB pipeline. Domain producers create a content package; channel publisher MCPs consume that stable contract.
9. **`kakao_token.py`** — Kakao OAuth token lifecycle: access_token auto-refresh via refresh_token, tokens stored in `kv_cache` (SQLite). Initial setup via `scripts/kakao_auth.py`

### Concurrency control

`scheduler.py` uses `fcntl.flock` for a file-based pipeline lock (`reports/runtime/pipeline.lock`). Manual runs can preempt scheduled runs via a manual override request file. Only one pipeline instance runs at a time.

### Two pipeline modes

- **Full pipeline** (`run_pipeline`): analysis → cache refresh → transactions → news → content generation → send
- **News-only pipeline** (`run_news_only_pipeline`): news → telegram content → send (skips KB analysis/transactions)

### Valuation sub-service

`valuation_web.py` is a Flask Blueprint registered at `/valuation` in `app.py`. It uses `valuation_service.py` and `valuation_db.py` for apartment complex relative-value analysis. Separate from the weekly pipeline.

### Database layer

`db_backend.py` provides a dual-backend database abstraction: local SQLite (`data/cache_store.sqlite3`) or remote Turso (libsql) based on env config. Used for transaction caching and valuation data.

### LLM provider routing

`reporters/common.py` selects provider per task via `DEFAULT_TASK_MODELS` dict, overridable by `REPORTER_{TASK}_PROVIDER` / `REPORTER_{TASK}_MODEL` env vars. Provider `"none"` skips LLM and uses template fallback text. Manual and scheduled `run_pipeline()` calls default to explicit LLM-disabled overrides, so API cost is only incurred when a caller passes LLM overrides that enable a task. All LLM calls are raw HTTP POST (no SDK dependencies for OpenAI/Gemini).

### Environment

See `.env.example` for all variables. Key groups:
- **Naver API**: `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET` (required for news)
- **LLM keys**: `OPENAI_API_KEY`, `GEMINI_API_KEY`, `ANTHROPIC_API_KEY` (optional; falls back to templates)
- **Delivery**: `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`, `SOLAPI_*` credentials, `KAKAO_REST_API_KEY`/`KAKAO_CLIENT_SECRET`
- **Pipeline channel toggles**: `SEND_TELEGRAM_ENABLED` (default true), `SEND_SMS_ENABLED` (default false), `SEND_KAKAO_ENABLED` (default false)
- **Instagram publisher MCP**: `INSTAGRAM_PUBLISHING_ENABLED` (default false), `INSTAGRAM_ACCESS_TOKEN`, `INSTAGRAM_ACCOUNT_ID`, `INSTAGRAM_GRAPH_API_VERSION`
- **Scheduler**: `ENABLE_SCHEDULER=1` activates APScheduler (Friday 10:30 KST)
- **Database**: `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN` for remote DB; falls back to local SQLite

### Deployment

Railway via `railway.json`. Health check at `/health`. Single instance recommended due to file-based locking.
