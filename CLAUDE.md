# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
pip install -r requirements.txt

# CLI dry-run (no external sends)
python scripts/run_local_pipeline_test.py

# CLI with custom params
python scripts/run_local_pipeline_test.py --news-days 1 --news-max-articles 3 --transaction-limit 2

# CLI with actual sends (Telegram/SMS will fire)
python scripts/run_local_pipeline_test.py --send

# Local web UI (manual trigger via browser at http://127.0.0.1:5000)
python scripts/run_local_web.py

# Local web UI with APScheduler enabled
python scripts/run_local_web.py --with-scheduler

# Production (Railway)
gunicorn app:app --bind 0.0.0.0:$PORT --worker-class gthread --workers 1 --threads 8 --timeout 0
```

No test suite exists. Validation is done via `run_local_pipeline_test.py` dry-runs.

## Architecture

Weekly pipeline that downloads KB real estate data, enriches it with transactions and news, generates multi-platform content via LLM, and delivers via Telegram/SMS.

### Pipeline flow (`scheduler.py` → `run_pipeline()`)

1. **`analyzer.py`** — Downloads KB weekly Excel from `api.kbland.kr`, parses 매매/전세 sheets with openpyxl+pandas, computes deltas, produces 8 regional content buckets (capital/non-capital × sale/rent × top/bottom 5)
2. **`realestate.py`** — Resolves region names to KB scope codes, queries KB API for 84㎡/59㎡ apartment transactions per region. Uses `ThreadPoolExecutor` (max 6 workers) for parallel region lookups. Caches via `valuation_db`
3. **`news.py`** — Queries Naver Search API for real estate keywords, filters by target publishers, scores by keyword priority. Resolves article URLs from Naver newspaper viewer format to standard URLs
4. **`reporter.py`** — Orchestrator that delegates to `reporters/` submodules for each platform
5. **`reporters/common.py`** — LLM provider routing (OpenAI/Gemini/Anthropic via raw HTTP, no SDKs), prompt assembly, URL placeholder protection to prevent LLM URL corruption, fallback text generation
6. **`reporters/{telegram,instagram,cardnews,blog,alimtalk}.py`** — Platform-specific prompt builders and generators
7. **`sender.py`** — Telegram Bot API delivery with auto-chunking (3900 char limit), SOLAPI SMS/LMS delivery. Instagram not yet implemented

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

`reporters/common.py` selects provider per task via `DEFAULT_TASK_MODELS` dict, overridable by `REPORTER_{TASK}_PROVIDER` / `REPORTER_{TASK}_MODEL` env vars. Backup models auto-fallback on primary failure. Provider `"none"` skips LLM and uses template fallback text. All LLM calls are raw HTTP POST (no SDK dependencies for OpenAI/Gemini).

### Environment

See `.env.example` for all variables. Key groups:
- **Naver API**: `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET` (required for news)
- **LLM keys**: `OPENAI_API_KEY`, `GEMINI_API_KEY`, `ANTHROPIC_API_KEY` (optional; falls back to templates)
- **Delivery**: `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`, `SOLAPI_*` credentials
- **Channel toggles**: `SEND_TELEGRAM_ENABLED` (default true), `SEND_SMS_ENABLED` (default false), `SEND_INSTAGRAM_ENABLED` (default false)
- **Scheduler**: `ENABLE_SCHEDULER=1` activates APScheduler (Friday 10:30 KST)
- **Database**: `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN` for remote DB; falls back to local SQLite

### Deployment

Railway via `railway.json`. Health check at `/health`. Single instance recommended due to file-based locking.
