from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mcp_runtime import ToolSpec

from .authoring import AuthoringAgent
from .common import ArtifactRepository
from .contracts import AgentDescriptor
from .data import DataAgent
from .director import DirectorAgent
from .news import NewsAgent
from .ops import OpsAgent
from .publishing import PublishingAgent
from .quality import QualityAgent
from .runtime import SafePipelineRunner
from .transactions import TransactionAgent


@dataclass
class AgentSystem:
    director: DirectorAgent
    data: DataAgent
    news: NewsAgent
    transactions: TransactionAgent
    authoring: AuthoringAgent
    quality: QualityAgent
    publishing: PublishingAgent
    ops: OpsAgent

    def descriptors(self) -> list[AgentDescriptor]:
        return [
            self.director.descriptor,
            self.data.descriptor,
            self.news.descriptor,
            self.transactions.descriptor,
            self.authoring.descriptor,
            self.quality.descriptor,
            self.publishing.descriptor,
            self.ops.descriptor,
        ]

    def tool_specs(self) -> list[ToolSpec]:
        tools = [
            *self.director.tool_specs(),
            *self.authoring.tool_specs(),
            *self.data.tool_specs(),
            *self.news.tool_specs(),
            *self.transactions.tool_specs(),
            *self.quality.tool_specs(),
            *self.ops.tool_specs(),
        ]
        names = [tool.name for tool in tools]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate MCP tool names: {duplicates}")
        return tools


@dataclass
class PipelineAgents:
    data: DataAgent
    news: NewsAgent
    transactions: TransactionAgent
    authoring: AuthoringAgent
    quality: QualityAgent
    publishing: PublishingAgent


def build_pipeline_agents(base_dir: str | Path) -> PipelineAgents:
    artifacts = ArtifactRepository(base_dir)
    return PipelineAgents(
        data=DataAgent(artifacts),
        news=NewsAgent(artifacts),
        transactions=TransactionAgent(artifacts),
        authoring=AuthoringAgent(artifacts),
        quality=QualityAgent(artifacts),
        publishing=PublishingAgent(),
    )


def build_agent_system(base_dir: str | Path) -> AgentSystem:
    artifacts = ArtifactRepository(base_dir)
    data = DataAgent(artifacts)
    news = NewsAgent(artifacts)
    transactions = TransactionAgent(artifacts)
    authoring = AuthoringAgent(artifacts, SafePipelineRunner())
    quality = QualityAgent(artifacts)
    publishing = PublishingAgent()

    system_holder: dict[str, AgentSystem] = {}
    ops = OpsAgent(
        artifacts,
        descriptor_provider=lambda: system_holder["system"].descriptors(),
    )
    director = DirectorAgent(
        authoring=authoring,
        data=data,
        news=news,
        transactions=transactions,
        quality=quality,
        ops=ops,
    )
    system = AgentSystem(
        director=director,
        data=data,
        news=news,
        transactions=transactions,
        authoring=authoring,
        quality=quality,
        publishing=publishing,
        ops=ops,
    )
    system_holder["system"] = system
    return system
