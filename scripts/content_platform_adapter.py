#!/usr/bin/env python3
"""Read-only KB adapter for the shared content automation platform.

The default ``collect`` command only reads ``reports/data_snapshot.json``.
Refreshing is explicit and goes through the project's existing no-send/no-LLM
pipeline boundary.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Sequence


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SNAPSHOT_PATH = BASE_DIR / "reports" / "data_snapshot.json"
SCHEMA_VERSION = "source-bundle/v1"
DOMAIN = "kb-realestate"


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(payload: Any) -> str:
    return f"sha256:{hashlib.sha256(_canonical_bytes(payload)).hexdigest()}"


def load_snapshot(snapshot_path: Path) -> dict[str, Any]:
    if not snapshot_path.is_file():
        raise FileNotFoundError(f"KB data snapshot not found: {snapshot_path}")
    try:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid KB data snapshot JSON: {snapshot_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("KB data snapshot root must be a JSON object")
    return payload


def build_source_bundle(
    snapshot: dict[str, Any],
    snapshot_path: Path,
    *,
    refresh_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = snapshot.get("source") if isinstance(snapshot.get("source"), dict) else {}
    resolved_path = snapshot_path.resolve()
    artifacts: list[dict[str, Any]] = [
        {
            "kind": "data_snapshot",
            "path": str(resolved_path),
            "size_bytes": resolved_path.stat().st_size,
        }
    ]
    source_files = source.get("source_files")
    if isinstance(source_files, dict):
        artifacts.extend(
            {
                "kind": "source_file",
                "name": str(name),
                "path": str(path),
            }
            for name, path in sorted(source_files.items())
            if path
        )
    report_images = source.get("report_images")
    if isinstance(report_images, list):
        artifacts.extend(
            {
                "kind": "report_image",
                "path": str(path),
            }
            for path in report_images
            if path
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "domain": DOMAIN,
        "generated_at": snapshot.get("generated_at"),
        "input_digest": _sha256(snapshot),
        "facts": {
            "latest_date": snapshot.get("latest_date"),
            "analysis": snapshot.get("analysis") or {},
            "transactions": snapshot.get("transactions") or {},
            "news": snapshot.get("news") or [],
        },
        "artifacts": artifacts,
        "metadata": {
            "source": source,
            "generation_plan": snapshot.get("generation_plan") or {},
            "generation_meta": snapshot.get("generation_meta") or {},
            "adapter": {
                "project": DOMAIN,
                "refresh_performed": refresh_result is not None,
                "external_delivery_performed": False,
                "project_llm_called": False,
            },
            "refresh_result": refresh_result,
        },
    }


def run_safe_refresh(
    *,
    news_days: int,
    news_max_articles: int,
    transaction_limit: int,
    skip_transactions: bool,
) -> dict[str, Any]:
    if str(BASE_DIR) not in sys.path:
        sys.path.insert(0, str(BASE_DIR))

    # Some legacy project modules print progress to stdout. Keep stdout reserved
    # for the adapter's single JSON response.
    with contextlib.redirect_stdout(sys.stderr):
        from kb_agents.contracts import PipelineOptions
        from kb_agents.runtime import SafePipelineRunner

        result = SafePipelineRunner().run(
            PipelineOptions(
                news_days=news_days,
                news_max_articles=news_max_articles,
                transaction_limit=transaction_limit,
                skip_transactions=skip_transactions,
            ),
            output_mode="both",
        )

    if not result.get("success"):
        raise RuntimeError(f"Safe KB refresh failed: {result.get('error') or 'unknown error'}")
    if result.get("send_enabled") is not False:
        raise RuntimeError("Safe KB refresh rejected: delivery was not explicitly disabled")
    if result.get("llm_api_enabled"):
        raise RuntimeError("Safe KB refresh rejected: project LLM was not disabled")
    return result


def collect(
    snapshot_path: Path = DEFAULT_SNAPSHOT_PATH,
    *,
    refresh: bool = False,
    news_days: int = 7,
    news_max_articles: int = 12,
    transaction_limit: int = 5,
    skip_transactions: bool = True,
) -> dict[str, Any]:
    refresh_result = None
    if refresh:
        refresh_result = run_safe_refresh(
            news_days=news_days,
            news_max_articles=news_max_articles,
            transaction_limit=transaction_limit,
            skip_transactions=skip_transactions,
        )
    snapshot = load_snapshot(snapshot_path)
    return build_source_bundle(snapshot, snapshot_path, refresh_result=refresh_result)


def capabilities() -> dict[str, Any]:
    return {
        "schema_version": "adapter-capabilities/v1",
        "domain": DOMAIN,
        "commands": {
            "capabilities": {
                "description": "Describe this adapter without external calls.",
            },
            "collect": {
                "output_schema": SCHEMA_VERSION,
                "default_snapshot": str(DEFAULT_SNAPSHOT_PATH),
                "supports_refresh": True,
                "refresh_defaults": {
                    "skip_transactions": True,
                    "project_llm_enabled": False,
                    "external_delivery_enabled": False,
                },
            },
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="KB content-platform adapter")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("capabilities", help="print adapter capabilities")

    collect_parser = subparsers.add_parser("collect", help="emit a source-bundle/v1 JSON document")
    collect_parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT_PATH)
    collect_parser.add_argument("--refresh", action="store_true")
    collect_parser.add_argument("--news-days", type=int, default=7)
    collect_parser.add_argument("--news-max-articles", type=int, default=12)
    collect_parser.add_argument("--transaction-limit", type=int, default=5)
    transaction_group = collect_parser.add_mutually_exclusive_group()
    transaction_group.add_argument(
        "--skip-transactions",
        dest="skip_transactions",
        action="store_true",
        default=True,
        help="skip transaction collection (default)",
    )
    transaction_group.add_argument(
        "--include-transactions",
        dest="skip_transactions",
        action="store_false",
        help="include transaction collection during --refresh",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "capabilities":
            payload = capabilities()
        else:
            payload = collect(
                args.snapshot,
                refresh=args.refresh,
                news_days=args.news_days,
                news_max_articles=args.news_max_articles,
                transaction_limit=args.transaction_limit,
                skip_transactions=args.skip_transactions,
            )
    except Exception as exc:
        print(f"content-platform adapter error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
