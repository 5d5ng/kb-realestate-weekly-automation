from __future__ import annotations

import unittest
from datetime import datetime

from openpyxl import Workbook

from analyzer import _consecutive_positive_weeks, _parse_change_sheet, extract_content_regions
from realestate import _extract_grouped_region_names


class AnalyzerRegionHeaderTests(unittest.TestCase):
    def test_consecutive_positive_weeks_stops_on_zero_negative_or_missing(self) -> None:
        rows = [
            [datetime(2026, 6, 29), 0.2, 0.1, 0.1],
            [datetime(2026, 7, 6), 0.0, 0.1, 0.1],
            [datetime(2026, 7, 13), 0.3, -0.1, None],
            [datetime(2026, 7, 20), 0.4, 0.2, 0.2],
        ]
        self.assertEqual(2, _consecutive_positive_weeks(rows, 1))
        self.assertEqual(1, _consecutive_positive_weeks(rows, 2))
        self.assertEqual(1, _consecutive_positive_weeks(rows, 3))

    def test_content_regions_include_all_seoul_and_rank_positive_regions_by_latest_change(self) -> None:
        def item(region: str, current: float, weeks: int) -> dict:
            return {"region": region, "current": current, "delta": 0.0, "consecutive_rise_weeks": weeks}

        seoul = [item(f"서울특별시 테스트구{index:02d}", index / 100, index) for index in range(1, 26)]
        capital = seoul + [item("경기도 상승시", 0.9, 2), item("인천광역시 보합구", 0.0, 0)]
        non_capital = [item(f"부산광역시 상승구{index}", 1 - index / 100, index) for index in range(1, 7)]
        regions = extract_content_regions({"sale": capital + non_capital, "rent": capital + non_capital})

        self.assertEqual(25, len(regions["seoul_sale_all"]))
        self.assertEqual("서울특별시 테스트구25", regions["seoul_sale_all"][0]["region"])
        self.assertEqual("경기도 상승시", regions["capital_sale_top5"][0]["region"])
        self.assertEqual(5, len(regions["non_capital_sale_top5"]))
        self.assertTrue(all(item["current"] > 0 for item in regions["capital_sale_top5"]))

    def test_transaction_grouping_does_not_truncate_seoul_sections(self) -> None:
        grouped = _extract_grouped_region_names({
            "content_regions": {
                "seoul_sale_all": [{"region": f"서울특별시 테스트구{index:02d}"} for index in range(1, 26)],
                "capital_sale_top5": [{"region": f"경기도 테스트시{index}"} for index in range(1, 8)],
            }
        })
        self.assertIsNotNone(grouped)
        self.assertEqual(25, len(grouped["seoul_sale_all"]))
        self.assertEqual(5, len(grouped["capital_sale_top5"]))

    def test_new_kb_section_headers_do_not_inherit_previous_province(self) -> None:
        workbook = Workbook()
        sheet = workbook.active
        sheet.append([None] * 8)
        sheet.append(
            [
                "구분",
                "인천광역시",
                "전남광주통합특별시",
                "(구)광주광역시",
                "남구",
                "경기도",
                "강원특별자치도도",
                "원주시",
            ]
        )
        sheet.append([datetime(2026, 7, 13), 0.1, 0.2, 0.3, -0.01, 0.4, 0.5, -0.02])
        sheet.append([datetime(2026, 7, 20), 0.2, 0.3, 0.4, -0.02, 0.5, 0.6, -0.03])

        _, records = _parse_change_sheet(sheet)
        region_names = {record["region"] for record in records}

        self.assertIn("광주광역시 남구", region_names)
        self.assertIn("강원특별자치도 원주시", region_names)
        self.assertNotIn("인천광역시 남구", region_names)
        self.assertFalse(any("강원특별자치도도" in name for name in region_names))


if __name__ == "__main__":
    unittest.main()
