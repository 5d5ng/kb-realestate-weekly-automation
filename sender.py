"""
채널별 콘텐츠 발송
- 텔레그램
- 솔라피 SMS (비용 이슈로 기본 비활성화)
- 인스타그램 (Meta Graph API, 보류)
"""
from __future__ import annotations

import os
from typing import Any

import requests
from dotenv import load_dotenv

try:
    from solapi import SolapiMessageService
    from solapi.model.request.message import Message as SolapiMessage
except ImportError:  # pragma: no cover - optional dependency in local env
    SolapiMessageService = None
    SolapiMessage = None


load_dotenv()
load_dotenv(".env.example", override=False)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SOLAPI_API_KEY = os.getenv("SOLAPI_API_KEY")
SOLAPI_API_SECRET = os.getenv("SOLAPI_API_SECRET")
SOLAPI_SENDER = os.getenv("SOLAPI_SENDER")
SOLAPI_DEFAULT_RECIPIENTS = os.getenv("SOLAPI_DEFAULT_RECIPIENTS", "")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_INSTAGRAM_ID = os.getenv("META_INSTAGRAM_ID")


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on"}


SEND_TELEGRAM_ENABLED = _env_flag("SEND_TELEGRAM_ENABLED", True)
SEND_SMS_ENABLED = _env_flag("SEND_SMS_ENABLED", False)
SEND_INSTAGRAM_ENABLED = _env_flag("SEND_INSTAGRAM_ENABLED", False)


def _split_csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _normalize_phone_number(phone_number: str) -> str:
    digits = "".join(ch for ch in str(phone_number) if ch.isdigit())
    if digits.startswith("82"):
        digits = f"0{digits[2:]}"
    return digits


def _build_result(success: bool, detail: str, **extra: Any) -> dict[str, Any]:
    payload = {"success": success, "detail": detail}
    payload.update(extra)
    return payload


def _build_skipped_result(detail: str, **extra: Any) -> dict[str, Any]:
    payload = {"success": True, "skipped": True, "detail": detail}
    payload.update(extra)
    return payload


def _resolve_channel_enabled(override: bool | None, default: bool) -> bool:
    if override is None:
        return default
    return override


def _get_solapi_service() -> SolapiMessageService:
    if SolapiMessageService is None or SolapiMessage is None:
        raise RuntimeError("solapi 패키지가 설치되어 있지 않습니다.")
    if not SOLAPI_API_KEY or not SOLAPI_API_SECRET:
        raise RuntimeError("SOLAPI_API_KEY 또는 SOLAPI_API_SECRET 이 비어 있습니다.")
    return SolapiMessageService(SOLAPI_API_KEY, SOLAPI_API_SECRET)


def get_solapi_balance() -> dict[str, Any]:
    """솔라피 인증 및 잔액 확인"""
    try:
        balance = _get_solapi_service().get_balance()
    except Exception as exc:  # pragma: no cover - external API
        return _build_result(False, f"잔액 조회 실패: {exc}")

    return _build_result(
        True,
        "잔액 조회 성공",
        balance=float(balance.balance),
        point=float(balance.point),
    )


def send_telegram(message: str, enabled: bool | None = None) -> dict[str, Any]:
    """텔레그램 메시지 발송"""
    if not _resolve_channel_enabled(enabled, SEND_TELEGRAM_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 텔레그램 발송을 건너뜁니다.")
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return _build_result(False, "TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 가 비어 있습니다.")

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover - external API
        return _build_result(False, f"텔레그램 발송 실패: {exc}")

    if not payload.get("ok"):
        return _build_result(False, "텔레그램 API 응답이 실패로 반환되었습니다.", response=payload)

    result = payload.get("result", {})
    return _build_result(
        True,
        "텔레그램 발송 성공",
        message_id=result.get("message_id"),
        chat_id=result.get("chat", {}).get("id"),
    )


def send_sms(
    message: str,
    phone_numbers: list[str] | None = None,
    sender_number: str | None = None,
    subject: str | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    """솔라피 SMS/LMS 발송"""
    if not _resolve_channel_enabled(enabled, SEND_SMS_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 SMS 발송을 건너뜁니다.")
    recipients = phone_numbers or _split_csv(SOLAPI_DEFAULT_RECIPIENTS)
    sender = sender_number or SOLAPI_SENDER

    if not recipients:
        return _build_result(False, "수신번호가 없습니다. phone_numbers 또는 SOLAPI_DEFAULT_RECIPIENTS 를 설정하세요.")
    if not sender:
        return _build_result(False, "발신번호가 없습니다. sender_number 또는 SOLAPI_SENDER 를 설정하세요.")

    normalized_recipients = [_normalize_phone_number(number) for number in recipients]
    normalized_sender = _normalize_phone_number(sender)

    try:
        solapi_messages = [
            SolapiMessage(
                from_=normalized_sender,
                to=recipient,
                text=message,
                subject=subject,
            )
            for recipient in normalized_recipients
        ]
        response = _get_solapi_service().send(solapi_messages)
    except Exception as exc:  # pragma: no cover - external API
        return _build_result(False, f"SOLAPI SMS 발송 실패: {exc}")

    message_list = response.message_list or []
    failed_list = response.failed_message_list or []
    return _build_result(
        True,
        "SOLAPI SMS 발송 성공",
        group_id=response.group_info.group_id,
        total=response.group_info.count.total,
        sent=len(message_list),
        failed=len(failed_list),
        statuses=[
            {
                "message_id": item.message_id,
                "status_code": item.status_code,
                "status_message": item.status_message,
            }
            for item in message_list
        ],
        failed_messages=[
            {
                "to": item.to,
                "status_code": item.status_code,
                "status_message": item.status_message,
            }
            for item in failed_list
        ],
    )


def send_alimtalk(message: str, phone_numbers: list[str], enabled: bool | None = None) -> dict[str, Any]:
    """현재는 알림톡 대신 동일 문안을 SMS로 발송"""
    return send_sms(message=message, phone_numbers=phone_numbers, enabled=enabled)


def post_instagram(
    caption: str,
    image_url: str | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    """인스타그램 게시물 업로드 (Meta Graph API)"""
    if not _resolve_channel_enabled(enabled, SEND_INSTAGRAM_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 인스타그램 업로드를 건너뜁니다.")
    if not META_ACCESS_TOKEN or not META_INSTAGRAM_ID:
        return _build_result(False, "인스타그램 계정/토큰 미설정으로 테스트를 건너뜁니다.")
    return _build_result(False, "인스타그램 업로드는 아직 구현되지 않았습니다.", image_url=image_url, caption=caption)


def send_all(
    contents: dict,
    phone_numbers: list[str] | None = None,
    sender_number: str | None = None,
    image_url: str | None = None,
    channel_overrides: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """전체 채널 발송 및 결과 반환"""
    telegram_message = contents.get("telegram_report")
    sms_message = contents.get("sms_message") or contents.get("alimtalk_message")
    instagram_caption = contents.get("instagram_caption")
    channel_overrides = channel_overrides or {}

    results = {
        "telegram": _build_result(False, "telegram_report 가 비어 있습니다."),
        "sms": _build_result(False, "alimtalk_message 또는 sms_message 가 비어 있습니다."),
        "instagram": _build_result(False, "instagram_caption 이 비어 있습니다."),
    }

    if telegram_message:
        results["telegram"] = send_telegram(
            telegram_message,
            enabled=channel_overrides.get("telegram"),
        )
    if sms_message:
        results["sms"] = send_sms(
            message=sms_message,
            phone_numbers=phone_numbers,
            sender_number=sender_number,
            enabled=channel_overrides.get("sms"),
        )
    if instagram_caption:
        results["instagram"] = post_instagram(
            instagram_caption,
            image_url=image_url,
            enabled=channel_overrides.get("instagram"),
        )

    return results


if __name__ == "__main__":
    from pprint import pprint

    pprint(
        {
            "solapi_balance": get_solapi_balance(),
            "telegram": send_telegram("[KB자동화 테스트] sender.py 연결 확인"),
            "sms": send_sms("[KB자동화 테스트] SOLAPI SMS 연결 확인"),
        }
    )
