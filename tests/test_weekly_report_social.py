from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.weekly_report_social import (
    load_and_validate_report,
    load_social_copy,
    parse_weekly_report,
)
from tests.test_desktop_content_package import fixture_files


class WeeklyReportSocialTests(unittest.TestCase):
    def test_report_is_primary_source_and_snapshot_only_validates_it(self):
        with tempfile.TemporaryDirectory() as root:
            report_path, snapshot_path, _ = fixture_files(Path(root))
            report = load_and_validate_report(report_path, snapshot_path)
            self.assertEqual(6, len(report["buckets"]))
            self.assertEqual([25, 25, 5, 5, 5, 5], [len(bucket["metrics"]) for bucket in report["buckets"]])
            self.assertEqual(3, len(report["news"]))
            self.assertTrue(report["report_sha256"].startswith("sha256:"))

    def test_changed_report_fact_is_rejected_by_snapshot_validation(self):
        with tempfile.TemporaryDirectory() as root:
            report_path, snapshot_path, _ = fixture_files(Path(root))
            text = report_path.read_text(encoding="utf-8").replace("| 1 | 수도권 매매 상승 상위 5 지역1 | 1주 |", "| 1 | 수도권 매매 상승 상위 5 지역1 | 999주 |", 1)
            report_path.write_text(text, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "consecutive rise weeks differ"):
                load_and_validate_report(report_path, snapshot_path)

    def test_social_copy_rejects_ungrounded_number(self):
        with tempfile.TemporaryDirectory() as root:
            report_path, _, social_copy_path = fixture_files(Path(root))
            report = parse_weekly_report(report_path)
            payload = json.loads(social_copy_path.read_text(encoding="utf-8"))
            payload["caption"] = "보고서에 없는 9999개 수치"
            social_copy_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not grounded"):
                load_social_copy(social_copy_path, report)


if __name__ == "__main__":
    unittest.main()
