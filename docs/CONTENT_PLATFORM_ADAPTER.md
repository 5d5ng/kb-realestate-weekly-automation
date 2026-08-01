# Content platform KB adapter

`scripts/content_platform_adapter.py` exposes KB data to the shared content
automation platform without coupling that platform to KB implementation modules.

The default command is read-only. It converts the latest local snapshot to one
`source-bundle/v1` JSON document on stdout and performs no network, LLM, or
publishing calls.

```bash
python3 scripts/content_platform_adapter.py capabilities
python3 scripts/content_platform_adapter.py collect
```

An explicit refresh uses the existing safe pipeline boundary. Delivery,
prompt-file delivery, and project LLM calls remain disabled. Transaction lookup
is skipped unless it is explicitly requested.

```bash
python3 scripts/content_platform_adapter.py collect --refresh
python3 scripts/content_platform_adapter.py collect --refresh --include-transactions
```

Successful commands write exactly one JSON value to stdout. Progress and errors
go to stderr, and an error exits nonzero. APScheduler is also opt-in: the web
process only starts it when `ENABLE_SCHEDULER` is explicitly set to `1`, `true`,
`yes`, or `on`.
