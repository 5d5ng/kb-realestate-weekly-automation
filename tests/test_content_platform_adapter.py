from __future__ import annotations

import json
import io
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from app import _should_enable_scheduler
from scripts.content_platform_adapter import build_source_bundle, collect, main


class SchedulerOptInTests(unittest.TestCase):
    def test_scheduler_is_disabled_when_environment_variable_is_missing(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(_should_enable_scheduler())

    def test_scheduler_requires_an_explicit_truthy_value(self) -> None:
        for value in ("1", "true", "YES", "on"):
            with self.subTest(value=value), mock.patch.dict(
                os.environ, {"ENABLE_SCHEDULER": value}, clear=True
            ):
                self.assertTrue(_should_enable_scheduler())

        for value in ("0", "false", "no", "off", "unexpected", ""):
            with self.subTest(value=value), mock.patch.dict(
                os.environ, {"ENABLE_SCHEDULER": value}, clear=True
            ):
                self.assertFalse(_should_enable_scheduler())


class ContentPlatformAdapterTests(unittest.TestCase):
    def _snapshot(self) -> dict:
        return {
            "generated_at": "2026-07-31 10:00:00",
            "latest_date": "2026-07-27",
            "source": {
                "source_files": {"sale": "/fixture/sale.xlsx"},
                "report_images": ["/fixture/page-1.png"],
            },
            "analysis": {"sale": {"서울": 1.25}, "rent": {}},
            "transactions": {},
            "news": [{"title": "KB 주간 뉴스", "url": "https://example.test/news"}],
            "generation_plan": {},
            "generation_meta": {},
        }

    def _write_snapshot(self, root: Path, payload: dict, *, pretty: bool = False) -> Path:
        path = root / "data_snapshot.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None),
            encoding="utf-8",
        )
        return path

    def test_digest_is_deterministic_across_json_formatting(self) -> None:
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            snapshot = self._snapshot()
            first_path = self._write_snapshot(Path(first_dir), snapshot, pretty=False)
            reordered = dict(reversed(list(snapshot.items())))
            second_path = self._write_snapshot(Path(second_dir), reordered, pretty=True)

            first = build_source_bundle(snapshot, first_path)
            second = build_source_bundle(reordered, second_path)

        self.assertEqual(first["input_digest"], second["input_digest"])
        self.assertTrue(first["input_digest"].startswith("sha256:"))

    def test_collect_reads_fixture_without_refresh_network_or_sender_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = self._write_snapshot(Path(temp_dir), self._snapshot())
            with mock.patch(
                "scripts.content_platform_adapter.run_safe_refresh",
                side_effect=AssertionError("refresh must not run"),
            ), mock.patch(
                "sender.send_telegram",
                side_effect=AssertionError("sender must not run"),
            ):
                bundle = collect(path)

        self.assertEqual(bundle["schema_version"], "source-bundle/v1")
        self.assertEqual(bundle["domain"], "kb-realestate")
        self.assertEqual(
            set(bundle),
            {
                "schema_version",
                "domain",
                "generated_at",
                "input_digest",
                "facts",
                "artifacts",
                "metadata",
            },
        )
        self.assertIsInstance(bundle["facts"], dict)
        self.assertIsInstance(bundle["artifacts"], list)
        self.assertIsInstance(bundle["metadata"], dict)
        self.assertEqual(bundle["artifacts"][0]["kind"], "data_snapshot")
        self.assertEqual(bundle["facts"]["latest_date"], "2026-07-27")
        self.assertFalse(bundle["metadata"]["adapter"]["external_delivery_performed"])
        self.assertFalse(bundle["metadata"]["adapter"]["project_llm_called"])

    def test_refresh_boundary_forces_no_send_no_llm_and_skips_transactions(self) -> None:
        fake_result = {
            "success": True,
            "send_enabled": False,
            "llm_overrides": {
                "telegram_report": False,
                "instagram_caption": False,
                "card_news_script": False,
            },
            "analysis_summary": {},
            "transaction_summary": {},
            "news_summary": {},
            "contents_summary": {},
            "authoring_files": {},
        }
        with mock.patch("scheduler.run_pipeline", return_value=fake_result) as run_pipeline:
            from scripts.content_platform_adapter import run_safe_refresh

            result = run_safe_refresh(
                news_days=7,
                news_max_articles=12,
                transaction_limit=5,
                skip_transactions=True,
            )

        self.assertTrue(result["success"])
        kwargs = run_pipeline.call_args.kwargs
        self.assertFalse(kwargs["send"])
        self.assertFalse(kwargs["send_prompt_files"])
        self.assertEqual(kwargs["channel_overrides"], {})
        self.assertTrue(kwargs["skip_transactions"])
        self.assertFalse(any(kwargs["llm_overrides"].values()))
        self.assertEqual(kwargs["output_mode"], "both")

    def test_cli_reserves_stdout_for_json_and_reports_errors_to_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = self._write_snapshot(root, self._snapshot())
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main(["collect", "--snapshot", str(path)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(stderr.getvalue(), "")
            self.assertEqual(json.loads(stdout.getvalue())["schema_version"], "source-bundle/v1")

            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main(["collect", "--snapshot", str(root / "missing.json")])

        self.assertNotEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("data snapshot not found", stderr.getvalue().lower())


if __name__ == "__main__":
    unittest.main()
