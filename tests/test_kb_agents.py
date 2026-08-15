from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from kb_agents.common import ArtifactRepository
from kb_agents.news import NewsAgent
from kb_agents.quality import QualityAgent
from kb_agents.registry import build_agent_system
from kb_agents.transactions import TransactionAgent
from mcp_runtime import tool_result_text


def _tool_text(result: dict) -> str:
    return str(result["content"][0]["text"])


def _tool_json(result: dict) -> dict:
    text = _tool_text(result)
    start = text.find("{")
    return json.loads(text[start:])


class FakeAuthoringAgent:
    def __init__(self) -> None:
        self.last_package_args: dict | None = None

    def generate_authoring_package(self, args: dict) -> dict:
        self.last_package_args = args
        return tool_result_text(json.dumps({"success": True}))

    def generate_weekly_report(self, args: dict) -> dict:
        return tool_result_text(json.dumps({"success": True}))

    def get_latest_package(self, args: dict) -> dict:
        return tool_result_text("package")

    def get_latest_weekly_report(self, args: dict) -> dict:
        return tool_result_text("report")


class AgentRegistryTests(unittest.TestCase):
    def test_registry_exposes_unique_tools_and_all_agent_roles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            system = build_agent_system(temp_dir)
            tool_names = [tool.name for tool in system.tool_specs()]
            agent_keys = [descriptor.key for descriptor in system.descriptors()]

        self.assertEqual(len(tool_names), len(set(tool_names)))
        self.assertEqual(
            set(agent_keys),
            {
                "director",
                "data",
                "news",
                "transactions",
                "authoring",
                "quality",
                "publishing",
                "ops",
            },
        )
        self.assertTrue(
            {
                "kb_realestate_assistant",
                "get_latest_news",
                "get_latest_transactions",
                "check_latest_artifacts",
                "get_agent_status",
            }.issubset(tool_names)
        )

    def test_director_routes_korean_request_and_transaction_intent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            system = build_agent_system(temp_dir)
            fake_authoring = FakeAuthoringAgent()
            system.director.authoring = fake_authoring

            result = system.director.handle(
                {
                    "request": "KB부동산 작성 패키지 만들어줘. 실거래 포함해.",
                    "news_days": 2,
                }
            )

        self.assertFalse(result["isError"])
        self.assertIn("담당 에이전트: 작성 에이전트", _tool_text(result))
        self.assertIsNotNone(fake_authoring.last_package_args)
        self.assertFalse(fake_authoring.last_package_args["skip_transactions"])
        self.assertEqual(fake_authoring.last_package_args["news_days"], 2)


class SnapshotAgentTests(unittest.TestCase):
    def _write_snapshot(self, root: Path) -> None:
        reports = root / "reports"
        reports.mkdir(parents=True)
        (reports / "data_snapshot.json").write_text(
            json.dumps(
                {
                    "generated_at": "2026-07-29 10:00:00",
                    "latest_date": "2026-07-27",
                    "source": {},
                    "analysis": {},
                    "news": [
                        {"title": "첫 기사", "publisher": "A"},
                        {"title": "둘째 기사", "publisher": "B"},
                    ],
                    "transactions": {
                        "capital_sale_top5": {
                            "서울특별시 강남구": {
                                "84": {"trades": [{"price": 100}]},
                            }
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def test_news_and_transaction_agents_read_their_own_snapshot_sections(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_snapshot(root)
            artifacts = ArtifactRepository(root)

            news_payload = _tool_json(NewsAgent(artifacts).get_latest_news({"limit": 1}))
            transaction_payload = _tool_json(
                TransactionAgent(artifacts).get_latest_transactions({})
            )

        self.assertEqual(news_payload["count"], 2)
        self.assertEqual([item["title"] for item in news_payload["items"]], ["첫 기사"])
        self.assertEqual(transaction_payload["summary"]["bucket_count"], 1)
        self.assertEqual(transaction_payload["summary"]["trade_count"], 1)

    def test_quality_agent_validates_artifact_structure_without_external_calls(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._write_snapshot(root)
            reports = root / "reports"
            report_body = "# 기준일 2026-07-27\n\n" + ("검토 가능한 본문입니다. " * 30)
            (reports / "llm_package.md").write_text(report_body, encoding="utf-8")
            (reports / "weekly_report.md").write_text(report_body, encoding="utf-8")

            payload = _tool_json(
                QualityAgent(ArtifactRepository(root)).check_latest_artifacts(
                    {"require_weekly_report": True}
                )
            )

        self.assertTrue(payload["success"])
        self.assertTrue(payload["ready_for_llm"])
        self.assertFalse(payload["external_calls_performed"])


if __name__ == "__main__":
    unittest.main()
