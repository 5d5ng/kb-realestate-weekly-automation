from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AgentDescriptor:
    key: str
    korean_name: str
    responsibility: str
    capabilities: tuple[str, ...]
    external_side_effects: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "korean_name": self.korean_name,
            "responsibility": self.responsibility,
            "capabilities": list(self.capabilities),
            "external_side_effects": self.external_side_effects,
        }


@dataclass(frozen=True)
class PipelineOptions:
    news_days: int = 1
    news_max_articles: int = 3
    transaction_limit: int = 1
    skip_transactions: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "news_days": self.news_days,
            "news_max_articles": self.news_max_articles,
            "transaction_limit": self.transaction_limit,
            "skip_transactions": self.skip_transactions,
        }
