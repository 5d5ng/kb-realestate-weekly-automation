from __future__ import annotations

from typing import Any

from .contracts import AgentDescriptor


class PublishingAgent:
    descriptor = AgentDescriptor(
        key="publishing",
        korean_name="게시 에이전트",
        responsibility="생성된 콘텐츠의 채널 발송을 담당하며 MCP 기본 경로에서는 호출되지 않습니다.",
        capabilities=("채널 발송", "게시 MCP로 승인 작업 위임"),
        external_side_effects=True,
    )

    def send(
        self,
        contents: dict[str, Any],
        *,
        channel_overrides: dict[str, bool] | None,
        send_prompt_files: bool | None,
    ) -> dict[str, Any]:
        from sender import send_all

        return send_all(
            contents,
            channel_overrides=channel_overrides,
            send_prompt_files=send_prompt_files,
        )
