from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from scheduler import KST
from scripts import finalize_desktop_weekly, run_scheduled_pipeline


class DesktopFinalizerTests(unittest.TestCase):
    def test_finalizer_requires_sixteen_canva_pages(self) -> None:
        finalize_desktop_weekly._validate_canva_page_count(16)
        with self.assertRaisesRegex(ValueError, "16"):
            finalize_desktop_weekly._validate_canva_page_count(10)

    def test_successful_receipt_requires_matching_digest_and_message_ids(self) -> None:
        receipt = {
            "success": True,
            "artifact_digest": "same",
            "telegram": {"success": True, "message_ids": [101]},
        }
        self.assertTrue(finalize_desktop_weekly._successful_receipt(receipt, "same"))
        self.assertFalse(finalize_desktop_weekly._successful_receipt(receipt, "different"))
        receipt["telegram"]["message_ids"] = []
        self.assertFalse(finalize_desktop_weekly._successful_receipt(receipt, "same"))

    def test_delivery_ok_requires_real_message_ids(self) -> None:
        result = {
            "success": True,
            "send_results": {
                "telegram": {"success": True, "message_ids": [201]},
                "telegram_prompt_files": {"success": True, "message_ids": [202]},
            },
        }
        self.assertTrue(run_scheduled_pipeline._delivery_ok(result))
        result["send_results"]["telegram"]["message_ids"] = []
        self.assertFalse(run_scheduled_pipeline._delivery_ok(result))

    def test_fallback_skips_only_a_successful_receipt_from_today(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "receipt.json"
            receipt_path.write_text(
                json.dumps(
                    {
                        "success": True,
                        "completed_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                        "telegram": {"success": True, "message_ids": [301]},
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(run_scheduled_pipeline, "DESKTOP_RECEIPT_PATH", receipt_path):
                receipt = run_scheduled_pipeline._desktop_delivery_succeeded_today()
            self.assertIsNotNone(receipt)

            payload = json.loads(receipt_path.read_text(encoding="utf-8"))
            payload["telegram"]["message_ids"] = []
            receipt_path.write_text(json.dumps(payload), encoding="utf-8")
            with patch.object(run_scheduled_pipeline, "DESKTOP_RECEIPT_PATH", receipt_path):
                self.assertIsNone(run_scheduled_pipeline._desktop_delivery_succeeded_today())


if __name__ == "__main__":
    unittest.main()
