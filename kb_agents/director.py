from __future__ import annotations

from typing import Any

from mcp_runtime import ToolSpec, tool_result_text

from .authoring import AuthoringAgent
from .common import as_bool, number_schema, pipeline_options_from_args
from .data import DataAgent
from .news import NewsAgent
from .ops import OpsAgent
from .quality import QualityAgent
from .transactions import TransactionAgent
from .contracts import AgentDescriptor


class DirectorAgent:
    descriptor = AgentDescriptor(
        key="director",
        korean_name="디렉터 에이전트",
        responsibility="한국어 요청을 해석하고 적절한 전문 에이전트에 작업을 위임합니다.",
        capabilities=("한국어 요청 분류", "전문 에이전트 위임", "한국어 도움말"),
    )

    ACTIONS = {
        "auto",
        "help",
        "make_package",
        "make_report",
        "read_package",
        "read_report",
        "read_data",
        "read_news",
        "read_transactions",
        "check_quality",
        "list_files",
        "agent_status",
    }

    def __init__(
        self,
        *,
        authoring: AuthoringAgent,
        data: DataAgent,
        news: NewsAgent,
        transactions: TransactionAgent,
        quality: QualityAgent,
        ops: OpsAgent,
    ) -> None:
        self.authoring = authoring
        self.data = data
        self.news = news
        self.transactions = transactions
        self.quality = quality
        self.ops = ops

    @staticmethod
    def _request_text(args: dict[str, Any]) -> str:
        value = args.get("request", "")
        return "" if value is None else str(value).strip()

    @staticmethod
    def _has_any(text: str, keywords: tuple[str, ...]) -> bool:
        return any(keyword in text for keyword in keywords)

    @staticmethod
    def _result_text(tool_result: dict[str, Any]) -> str:
        content = tool_result.get("content")
        if not isinstance(content, list) or not content:
            return ""
        first = content[0]
        return str(first.get("text", "")) if isinstance(first, dict) else ""

    def _wrapped_result(
        self,
        action: str,
        detail: str,
        tool_result: dict[str, Any],
    ) -> dict[str, Any]:
        prefix = f"처리한 작업: {action}\n담당 에이전트: {detail}\n\n"
        return tool_result_text(
            prefix + self._result_text(tool_result),
            is_error=bool(tool_result.get("isError")),
        )

    def route_action(self, request: str, explicit_action: str) -> str:
        action = explicit_action.strip().lower()
        if action and action != "auto":
            if action not in self.ACTIONS:
                raise ValueError(f"action must be one of {sorted(self.ACTIONS)}")
            return action

        text = request.casefold()
        if not text:
            return "help"
        if self._has_any(text, ("도움", "사용법", "뭐할", "뭘 할", "help", "manual", "명령어")):
            return "help"
        if self._has_any(text, ("에이전트 상태", "에이전트 목록", "구조", "agent status")):
            return "agent_status"
        if self._has_any(text, ("품질", "검수", "오류 검사", "quality", "검증")):
            return "check_quality"
        if self._has_any(text, ("파일 목록", "목록", "산출물", "생성 파일", "어디", "artifact")):
            return "list_files"
        if self._has_any(text, ("실거래", "거래 내역", "transaction")) and self._has_any(
            text, ("읽", "보여", "확인", "가져", "불러")
        ):
            return "read_transactions"
        if self._has_any(text, ("뉴스", "기사")) and self._has_any(
            text, ("읽", "보여", "확인", "가져", "불러")
        ):
            return "read_news"
        if self._has_any(text, ("데이터", "스냅샷", "원본", "json", "snapshot")):
            return "read_data"

        create_words = ("만들", "생성", "실행", "갱신", "업데이트", "새로", "최신으로", "뽑", "준비")
        read_words = ("읽", "보여", "열어", "확인", "가져와", "불러", "봐줘")
        report_words = ("주간 보고서", "보고서 초안", "markdown", "마크다운", "weekly_report")
        package_words = ("작성 패키지", "패키지", "붙여넣", "블로그 자료", "글 쓸 자료", "자료", "llm_package")

        wants_create = self._has_any(text, create_words)
        wants_read = self._has_any(text, read_words)
        wants_report = self._has_any(text, report_words)
        wants_package = self._has_any(text, package_words)

        if wants_create and wants_report:
            return "make_report"
        if wants_create and (
            wants_package or self._has_any(text, ("블로그", "텔레그램", "kb부동산", "kb 부동산"))
        ):
            return "make_package"
        if wants_read and wants_report:
            return "read_report"
        if wants_read and (
            wants_package or self._has_any(text, ("블로그", "텔레그램"))
        ):
            return "read_package"
        if wants_report:
            return "make_report"
        if wants_package or self._has_any(text, ("블로그", "텔레그램", "kb부동산", "kb 부동산")):
            return "make_package"
        return "help"

    def _pipeline_args(self, args: dict[str, Any]) -> dict[str, Any]:
        request = self._request_text(args).casefold()
        if "skip_transactions" in args:
            skip_transactions = as_bool(args, "skip_transactions", True)
        elif self._has_any(
            request,
            ("실거래 포함", "실거래 조회", "실거래도", "거래 포함", "생략하지", "빼지 말"),
        ):
            skip_transactions = False
        else:
            skip_transactions = True
        return pipeline_options_from_args(
            args,
            skip_transactions=skip_transactions,
        ).as_dict()

    def help(self, args: dict[str, Any]) -> dict[str, Any]:
        del args
        return tool_result_text(
            """
# KB부동산 MCP 사용법

영어 도구명을 외우지 말고 `kb_realestate_assistant`에 한국어로 요청하면 디렉터가 담당 에이전트를 선택합니다.

- "최신 작성 패키지 만들어줘. 빠르게 실행하고 실거래는 생략해."
- "주간 보고서 초안까지 만들어줘."
- "최신 뉴스 보여줘."
- "최신 실거래 내역 보여줘."
- "생성 결과 품질 검사해줘."
- "에이전트 상태 보여줘."
- "생성된 파일 목록 보여줘."

담당 구분:
데이터는 데이터 에이전트, 뉴스는 뉴스 에이전트, 실거래는 실거래 에이전트,
보고서 작성은 작성 에이전트, 검수는 품질 에이전트, 상태 확인은 운영 에이전트가 처리합니다.

안전 기본값:
LLM API 호출 없음, 외부 발송 없음, 기본 실거래 조회 생략.
""".strip()
        )

    def handle(self, args: dict[str, Any]) -> dict[str, Any]:
        request = self._request_text(args)
        action = self.route_action(request, str(args.get("action", "auto") or "auto"))

        if action == "help":
            return self._wrapped_result("도움말 보기", "디렉터 에이전트", self.help({}))
        if action == "agent_status":
            return self._wrapped_result("에이전트 상태 확인", "운영 에이전트", self.ops.get_agent_status({}))
        if action == "list_files":
            return self._wrapped_result("생성 파일 목록 확인", "운영 에이전트", self.ops.list_artifacts({}))
        if action == "check_quality":
            return self._wrapped_result(
                "최신 산출물 품질 검사",
                "품질 검수 에이전트",
                self.quality.check_latest_artifacts(args),
            )
        if action == "read_data":
            return self._wrapped_result("데이터 스냅샷 읽기", "데이터 에이전트", self.data.get_data_snapshot({}))
        if action == "read_news":
            return self._wrapped_result("최신 뉴스 읽기", "뉴스 에이전트", self.news.get_latest_news(args))
        if action == "read_transactions":
            return self._wrapped_result(
                "최신 실거래 읽기",
                "실거래 에이전트",
                self.transactions.get_latest_transactions({}),
            )
        if action == "read_report":
            return self._wrapped_result(
                "주간 보고서 초안 읽기",
                "작성 에이전트",
                self.authoring.get_latest_weekly_report({}),
            )
        if action == "read_package":
            return self._wrapped_result(
                "작성 패키지 읽기",
                "작성 에이전트",
                self.authoring.get_latest_package({}),
            )
        if action == "make_report":
            return self._wrapped_result(
                "주간 보고서 초안 생성",
                "작성 에이전트",
                self.authoring.generate_weekly_report(self._pipeline_args(args)),
            )
        if action == "make_package":
            return self._wrapped_result(
                "작성 패키지 생성",
                "작성 에이전트",
                self.authoring.generate_authoring_package(self._pipeline_args(args)),
            )
        return self._wrapped_result("도움말 보기", "디렉터 에이전트", self.help({}))

    def tool_specs(self) -> list[ToolSpec]:
        return [
            ToolSpec(
                name="kb_help",
                description="디렉터 에이전트가 KB부동산 MCP의 한국어 사용법을 보여줍니다.",
                input_schema={"type": "object", "properties": {}, "additionalProperties": False},
                handler=self.help,
            ),
            ToolSpec(
                name="kb_realestate_assistant",
                description=(
                    "디렉터 에이전트가 한국어 요청을 해석해 데이터, 뉴스, 실거래, 작성, "
                    "품질, 운영 에이전트 중 알맞은 담당자에게 위임합니다."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "request": {
                            "type": "string",
                            "description": "한국어 자연어 요청.",
                            "default": "",
                        },
                        "action": {
                            "type": "string",
                            "description": "자동 분류가 헷갈릴 때만 지정합니다.",
                            "enum": sorted(self.ACTIONS),
                            "default": "auto",
                        },
                        "news_days": number_schema("수집할 뉴스 기간(일).", 1, 1, 30),
                        "news_max_articles": number_schema("수집할 최대 기사 수.", 3, 0, 30),
                        "transaction_limit": number_schema("지역/타입별 실거래 최대 수.", 1, 0, 20),
                        "skip_transactions": {
                            "type": "boolean",
                            "description": "true면 실거래 조회를 생략합니다.",
                            "default": True,
                        },
                        "limit": number_schema("뉴스 조회 시 가져올 최대 기사 수.", 10, 1, 30),
                        "require_weekly_report": {
                            "type": "boolean",
                            "description": "품질 검사 시 주간 보고서를 필수로 볼지 여부.",
                            "default": False,
                        },
                    },
                    "additionalProperties": False,
                },
                handler=self.handle,
            ),
        ]
