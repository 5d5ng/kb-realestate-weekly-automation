"""
멀티 LLM 기반 4종 콘텐츠 생성 오케스트레이터

플랫폼별 구현은 reporters 패키지로 분리한다.
"""
from __future__ import annotations

from reporters.alimtalk import generate_alimtalk_message
from reporters.blog import build_naver_blog_prompt
from reporters.cardnews import generate_card_news_script
from reporters.cardnews import build_card_news_prompt
from reporters.common import generation_override_context, get_generation_plan, save_prompt_file
from reporters.instagram import build_instagram_caption_prompt, generate_instagram_caption
from reporters.telegram import build_telegram_report_prompt, generate_telegram_report


def export_prompt_files(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    *,
    telegram_news_limit: int = 30,
) -> dict[str, str]:
    telegram_system, telegram_prompt = build_telegram_report_prompt(
        analysis,
        news,
        transactions,
        max_news_items=telegram_news_limit,
    )
    instagram_system, instagram_prompt = build_instagram_caption_prompt(analysis, news)
    cardnews_system, cardnews_prompt = build_card_news_prompt(analysis)
    blog_system, blog_prompt = build_naver_blog_prompt(analysis, news, transactions)

    return {
        "telegram_report": save_prompt_file("telegram_report", telegram_system, telegram_prompt),
        "instagram_caption": save_prompt_file("instagram_caption", instagram_system, instagram_prompt),
        "card_news_script": save_prompt_file("card_news_script", cardnews_system, cardnews_prompt),
        "naver_blog_post": save_prompt_file("naver_blog_post", blog_system, blog_prompt),
    }


def generate_all_contents(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    llm_overrides: dict | None = None,
    *,
    telegram_news_limit: int = 30,
) -> dict:
    """4종 콘텐츠 일괄 생성 + 검토용 프롬프트 파일 저장"""
    with generation_override_context(llm_overrides):
        return {
            "telegram_report": generate_telegram_report(
                analysis,
                news,
                transactions,
                max_news_items=telegram_news_limit,
            ),
            "alimtalk_message": generate_alimtalk_message(analysis),
            "instagram_caption": generate_instagram_caption(analysis, news),
            "card_news_script": generate_card_news_script(analysis),
            "generation_plan": get_generation_plan(),
            "prompt_files": export_prompt_files(
                analysis,
                news,
                transactions,
                telegram_news_limit=telegram_news_limit,
            ),
        }
