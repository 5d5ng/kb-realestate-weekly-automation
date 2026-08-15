"""
멀티 LLM 기반 4종 콘텐츠 생성 오케스트레이터

플랫폼별 구현은 reporters 패키지로 분리한다.
"""
from __future__ import annotations

from reporters.alimtalk import build_alimtalk_prompt, generate_alimtalk_message
from reporters.authoring import generate_authoring_artifacts, normalize_output_mode
from reporters.blog import build_naver_blog_prompt
from reporters.cardnews import build_card_news_prompt
from reporters.cardnews import generate_card_news_script
from reporters.common import (
    generation_meta_context,
    generation_override_context,
    get_generation_meta,
    get_generation_plan,
    save_prompt_file,
)
from reporters.instagram import build_instagram_caption_prompt, generate_instagram_caption
from reporters.telegram import (
    build_news_only_telegram_prompt,
    build_telegram_report_prompt,
    generate_news_only_telegram_report,
    generate_telegram_report,
)

REQUIRED_PROMPT_TASKS = ("telegram_report", "naver_blog_post")


def _build_prompt_save_payload(
    saved_prompts: dict[str, dict[str, str]],
    *,
    required_tasks: tuple[str, ...] = REQUIRED_PROMPT_TASKS,
) -> dict[str, dict[str, str] | dict[str, bool]]:
    prompt_files = {
        task_name: prompt_info["latest"]
        for task_name, prompt_info in saved_prompts.items()
        if prompt_info.get("latest")
    }
    prompt_archive_files = {
        task_name: prompt_info["archive"]
        for task_name, prompt_info in saved_prompts.items()
        if prompt_info.get("archive")
    }
    required_prompt_files = {
        task_name: prompt_files[task_name]
        for task_name in required_tasks
        if task_name in prompt_files
    }
    required_prompt_status = {
        task_name: bool(required_prompt_files.get(task_name))
        for task_name in required_tasks
    }
    return {
        "prompt_files": prompt_files,
        "prompt_archive_files": prompt_archive_files,
        "required_prompt_files": required_prompt_files,
        "required_prompt_status": required_prompt_status,
    }


def export_prompt_files(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    *,
    telegram_news_limit: int = 30,
) -> dict[str, dict[str, str] | dict[str, bool]]:
    telegram_system, telegram_prompt = build_telegram_report_prompt(
        analysis,
        news,
        transactions,
        max_news_items=telegram_news_limit,
    )
    instagram_system, instagram_prompt = build_instagram_caption_prompt(analysis, news, transactions)
    cardnews_system, cardnews_prompt = build_card_news_prompt(analysis, transactions)
    alimtalk_system, alimtalk_prompt = build_alimtalk_prompt(analysis, transactions)
    blog_system, blog_prompt = build_naver_blog_prompt(analysis, news, transactions)

    latest_date = analysis.get("latest_date", "")
    saved_prompts = {
        "telegram_report": save_prompt_file(
            "telegram_report",
            telegram_system,
            telegram_prompt,
            latest_date=latest_date,
        ),
        "naver_blog_post": save_prompt_file(
            "naver_blog_post",
            blog_system,
            blog_prompt,
            latest_date=latest_date,
        ),
        "instagram_caption": save_prompt_file(
            "instagram_caption",
            instagram_system,
            instagram_prompt,
            latest_date=latest_date,
        ),
        "card_news_script": save_prompt_file(
            "card_news_script",
            cardnews_system,
            cardnews_prompt,
            latest_date=latest_date,
        ),
        "alimtalk_message": save_prompt_file(
            "alimtalk_message",
            alimtalk_system,
            alimtalk_prompt,
            latest_date=latest_date,
        ),
    }
    return _build_prompt_save_payload(saved_prompts)


def generate_all_contents(
    analysis: dict,
    news: list[dict],
    transactions: dict | None = None,
    llm_overrides: dict | None = None,
    *,
    telegram_news_limit: int = 30,
    output_mode: str | None = "both",
) -> dict:
    """4종 콘텐츠 일괄 생성 + 검토용 프롬프트 파일 저장"""
    normalized_output_mode = normalize_output_mode(output_mode)
    with generation_override_context(llm_overrides):
        with generation_meta_context():
            prompt_payload = export_prompt_files(
                analysis,
                news,
                transactions,
                telegram_news_limit=telegram_news_limit,
            )
            payload = {
                "generation_plan": get_generation_plan(),
                **prompt_payload,
            }
            payload["telegram_report"] = generate_telegram_report(
                analysis,
                news,
                transactions,
                max_news_items=telegram_news_limit,
            )
            optional_generators = {
                "alimtalk_message": lambda: generate_alimtalk_message(analysis, transactions),
                "instagram_caption": lambda: generate_instagram_caption(analysis, news, transactions),
                "card_news_script": lambda: generate_card_news_script(analysis, transactions),
            }
            generation_errors: dict[str, str] = {}
            for task_name, generator in optional_generators.items():
                try:
                    payload[task_name] = generator()
                except Exception as exc:
                    generation_errors[task_name] = str(exc)
            if generation_errors:
                payload["generation_errors"] = generation_errors
            payload["generation_meta"] = get_generation_meta()
            payload.update(
                generate_authoring_artifacts(
                    analysis,
                    news,
                    transactions,
                    output_mode=normalized_output_mode,
                    generation_plan=payload["generation_plan"],
                    generation_meta=payload["generation_meta"],
                    card_news_script=payload.get("card_news_script"),
                )
            )
            return payload


def generate_news_only_contents(
    news: list[dict],
    llm_overrides: dict | None = None,
    *,
    telegram_news_limit: int = 30,
) -> dict:
    with generation_override_context(llm_overrides):
        with generation_meta_context():
            telegram_system, telegram_prompt = build_news_only_telegram_prompt(
                news,
                max_news_items=telegram_news_limit,
            )
            telegram_report = generate_news_only_telegram_report(
                news,
                max_news_items=telegram_news_limit,
            )
            saved_prompts = {
                "telegram_report": save_prompt_file(
                    "telegram_report",
                    telegram_system,
                    telegram_prompt,
                    fallback_text=telegram_report,
                )
            }
            payload = {
                "telegram_report": telegram_report,
                "sms_message": telegram_report,
                "generation_plan": get_generation_plan(),
                **_build_prompt_save_payload(saved_prompts, required_tasks=("telegram_report",)),
            }
            payload["generation_meta"] = get_generation_meta()
            return payload
