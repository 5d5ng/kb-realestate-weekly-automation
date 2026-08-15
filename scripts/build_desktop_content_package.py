#!/usr/bin/env python3
"""Assemble reviewed KB report-derived assets into content-package/v1.

``weekly_report.md`` is the only editorial source. ``data_snapshot.json`` is
read solely to cross-check the report, and the Codex Desktop social-copy file
must carry the exact report digest. No LLM, upload, or publication occurs here.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from scripts.weekly_report_social import load_and_validate_report, load_social_copy
except ModuleNotFoundError:  # Direct ``python scripts/...py`` execution.
    from weekly_report_social import load_and_validate_report, load_social_copy


BASE_DIR = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH = BASE_DIR / "reports" / "data_snapshot.json"
REPORT_PATH = BASE_DIR / "reports" / "weekly_report.md"


def build(
    report_path: Path,
    social_copy_path: Path,
    media_manifest_path: Path,
    snapshot_path: Path = SNAPSHOT_PATH,
) -> dict[str, Any]:
    report = load_and_validate_report(report_path, snapshot_path)
    social_copy = load_social_copy(social_copy_path, report)
    manifest = json.loads(media_manifest_path.read_text(encoding="utf-8"))
    manifest_media = manifest.get("media") or []
    if manifest.get("page_count") != 16 or len(manifest_media) != 16:
        raise ValueError("Canva review media manifest must contain exactly 16 pages")
    media = [
        {
            "type": str(item.get("type") or "image"),
            "source": str(item.get("source") or ""),
            "alt_text": str(item.get("alt_text") or ""),
        }
        for item in manifest_media
    ]
    if not all(Path(item["source"]).is_file() for item in media):
        raise FileNotFoundError("one or more Instagram media files are missing")

    caption = social_copy["caption"].strip()
    slides = [dict(item) for item in social_copy["slides"]]
    return {
        "schema_version": "content-package/v1",
        "title": report["title"],
        "content_type": "carousel",
        "caption": caption,
        "media": media,
        "targets": ["naver", "instagram"],
        "metadata": {
            "latest_date": report["latest_date"],
            "source_report_sha256": report["report_sha256"],
            "social_copy_schema": social_copy["schema_version"],
            "api_call_count": 0,
            "renderer_outputs": {
                "naver": {
                    "title": report["title"],
                    "body": report["body"],
                    "hashtags": ["KB부동산", "주간시계열", "부동산시장", "아파트실거래"],
                    "image_paths": [item["source"] for item in media],
                },
                "instagram": {
                    "caption": caption,
                    "slides": slides,
                },
            },
            "review_artifacts": {
                "primary_source": str(report_path.resolve()),
                "validation_source": str(snapshot_path.resolve()),
                "social_copy": str(social_copy_path.resolve()),
                "media_manifest": str(media_manifest_path.resolve()),
            },
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="검수한 weekly_report.md를 content-package 입력 JSON으로 조립")
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT_PATH)
    parser.add_argument("--social-copy", type=Path, required=True)
    parser.add_argument("--media-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    package = build(args.report, args.social_copy, args.media_manifest, args.snapshot)
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(package, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
