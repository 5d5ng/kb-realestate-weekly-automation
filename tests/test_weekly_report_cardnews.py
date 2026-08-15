from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.build_canva_weekly_cardnews import build
from tests.test_desktop_content_package import fixture_files


class WeeklyReportCardnewsTests(unittest.TestCase):
    def test_builds_sixteen_pages_from_report_and_codex_social_copy(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            report, snapshot, social_copy = fixture_files(base)
            output = base / "cardnews.html"
            result = build(report, social_copy, snapshot, output)
            document = result.read_text(encoding="utf-8")
            self.assertEqual(16, document.count('data-document-role="page"'))
            self.assertIn("카드 제목", document)
            self.assertIn("weekly_report.md 검증본", document)
            self.assertIn("서울 25개 구 매매 상승 현황 (1/5)", document)
            self.assertNotIn("변동률", document)
            self.assertNotIn("전주 대비", document)


if __name__ == "__main__":
    unittest.main()
