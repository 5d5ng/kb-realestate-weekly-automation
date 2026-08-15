from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts import build_desktop_content_package as builder
from scripts.weekly_report_social import BUCKETS


def fixture_files(base: Path) -> tuple[Path, Path, Path]:
    sections = []
    content_regions = {}
    transactions = {}
    for bucket_key, label in BUCKETS:
        metrics = []
        report_metrics = []
        report_trades = []
        bucket_transactions = {}
        region_count = 25 if bucket_key.startswith("seoul_") else 5
        for index in range(1, region_count + 1):
            region = "%s 지역%s" % (label, index)
            metrics.append({
                "region": region,
                "current": index / 100,
                "delta": index / 1000,
                "consecutive_rise_weeks": index,
            })
            report_metrics.append("| %s | %s | %s주 |" % (index, region, index))
            report_trades.append("| %s | 84타입 | 단지%s | 2026-08-01 | %s만원 | %s만원 |" % (
                region, index, 10000 + index, 5000 + index,
            ))
            bucket_transactions[region] = {
                "84": {
                    "trades": [{
                        "name": "단지%s" % index,
                        "contract_date": "2026-08-01",
                        "price": 10000 + index,
                        "related_rent_trades": [{"price": 5000 + index}],
                    }]
                }
            }
        content_regions[bucket_key] = metrics
        transactions[bucket_key] = bucket_transactions
        sections.append(
            "### %s\n\n보고서 근거를 요약합니다.\n\n"
            "| 순위 | 지역 | 연속 상승 |\n|---|---|---:|\n%s\n\n"
            "해당 버킷 실거래 표입니다.\n\n"
            "| 지역 | 타입 | 단지명 | 계약일 | 매매가 | 전세참고 |\n|---|---|---|---|---:|---:|\n%s"
            % (label, "\n".join(report_metrics), "\n".join(report_trades))
        )
    news = [
        {"title": "뉴스%s" % index, "publisher": "언론사", "resolved_url": "https://example.test/%s" % index}
        for index in range(1, 4)
    ]
    news_md = "\n".join(
        "%s. **%s**\n   - 출처: 언론사 | 2026-08-08\n   - 요약: 근거 요약\n   - 링크: %s"
        % (index, item["title"], item["resolved_url"])
        for index, item in enumerate(news, start=1)
    )
    report_text = (
        "# 2026-08-03 최신 KB 주간 브리프\n\n본문\n\n"
        "## 이번 주 핵심 요약\n\n핵심 요약\n\n## 6개 상승 섹션 상세\n\n%s\n\n"
        "## 이번 주 주요 뉴스\n\n%s\n\n## 한 줄 정리\n\n한 줄 결론\n\n"
        "## 참고 링크\n\n- KB: https://example.test/kb\n\n## 면책 문구\n\n투자 권유가 아닌 정보 제공용입니다."
        % ("\n\n".join(sections), news_md)
    )
    report = base / "weekly_report.md"
    report.write_text(report_text, encoding="utf-8")
    snapshot = base / "data_snapshot.json"
    snapshot.write_text(json.dumps({
        "latest_date": "2026-08-03",
        "analysis": {"content_regions": content_regions},
        "transactions": transactions,
        "news": news,
    }, ensure_ascii=False), encoding="utf-8")
    social_copy = base / "social-copy.json"
    social_copy.write_text(json.dumps({
        "schema_version": "kb-social-copy/v2",
        "source_report_sha256": "sha256:" + hashlib.sha256(report_text.encode("utf-8")).hexdigest(),
        "latest_date": "2026-08-03",
        "caption": "이번 주 부동산 흐름을 보고서 근거로 정리했습니다.",
        "slides": [
            {"position": index, "title": "카드 제목", "body": "보고서 근거 요약"}
            for index in range(1, 17)
        ],
    }, ensure_ascii=False), encoding="utf-8")
    return report, snapshot, social_copy


class DesktopContentPackageTests(unittest.TestCase):
    def test_builds_report_grounded_naver_and_sixteen_slide_review_package(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            report, snapshot, social_copy = fixture_files(base)
            media = []
            for index in range(1, 17):
                path = base / ("%02d.jpg" % index)
                path.write_bytes(b"jpeg")
                media.append({
                    "position": index, "type": "image", "source": str(path),
                    "alt_text": "카드 %s" % index,
                })
            manifest = base / "manifest.json"
            manifest.write_text(json.dumps({"page_count": 16, "media": media}), encoding="utf-8")

            package = builder.build(report, social_copy, manifest, snapshot)
            self.assertEqual("carousel", package["content_type"])
            self.assertEqual(16, len(package["media"]))
            self.assertEqual(16, len(package["metadata"]["renderer_outputs"]["instagram"]["slides"]))
            self.assertIn("본문", package["metadata"]["renderer_outputs"]["naver"]["body"])
            self.assertEqual("kb-social-copy/v2", package["metadata"]["social_copy_schema"])
            self.assertEqual(0, package["metadata"]["api_call_count"])
            self.assertNotIn("card_news_script", package["metadata"]["review_artifacts"])

    def test_rejects_social_copy_after_report_edit(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            report, snapshot, social_copy = fixture_files(base)
            report.write_text(report.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            manifest = base / "manifest.json"
            manifest.write_text(json.dumps({"page_count": 16, "media": []}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source_report_sha256"):
                builder.build(report, social_copy, manifest, snapshot)


if __name__ == "__main__":
    unittest.main()
