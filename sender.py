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


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on"}


def _env_text(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = str(value).strip()
    if normalized == "":
        return default
    return normalized


def _mask_secret(value: str | None, *, keep_prefix: int = 4, keep_suffix: int = 4) -> str | None:
    if not value:
        return None
    if len(value) <= keep_prefix + keep_suffix:
        return "*" * len(value)
    return f"{value[:keep_prefix]}...{value[-keep_suffix:]}"


def _telegram_config_status() -> dict[str, Any]:
    token = _env_text("TELEGRAM_BOT_TOKEN")
    chat_id = _env_text("TELEGRAM_CHAT_ID")
    return {
        "bot_token_present": bool(token),
        "chat_id_present": bool(chat_id),
        "bot_token_masked": _mask_secret(token),
        "chat_id_masked": _mask_secret(chat_id, keep_prefix=0, keep_suffix=4),
    }


def get_delivery_config_snapshot() -> dict[str, Any]:
    return {
        "telegram": _telegram_config_status(),
        "sms": {
            "api_key_present": bool(_env_text("SOLAPI_API_KEY")),
            "api_secret_present": bool(_env_text("SOLAPI_API_SECRET")),
            "sender_present": bool(_env_text("SOLAPI_SENDER")),
            "recipients_present": bool(_split_csv(_env_text("SOLAPI_DEFAULT_RECIPIENTS", ""))),
        },
        "instagram": {
            "access_token_present": bool(_env_text("META_ACCESS_TOKEN")),
            "instagram_id_present": bool(_env_text("META_INSTAGRAM_ID")),
        },
    }


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
    api_key = _env_text("SOLAPI_API_KEY")
    api_secret = _env_text("SOLAPI_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("SOLAPI_API_KEY 또는 SOLAPI_API_SECRET 이 비어 있습니다.")
    return SolapiMessageService(api_key, api_secret)


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
    token = _env_text("TELEGRAM_BOT_TOKEN")
    chat_id = _env_text("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        config_status = _telegram_config_status()
        missing = []
        if not token:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not chat_id:
            missing.append("TELEGRAM_CHAT_ID")
        return _build_result(
            False,
            f"텔레그램 환경변수가 비어 있습니다: {', '.join(missing)}",
            config_status=config_status,
        )

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
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
    recipients = phone_numbers or _split_csv(_env_text("SOLAPI_DEFAULT_RECIPIENTS", ""))
    sender = sender_number or _env_text("SOLAPI_SENDER")
    if phone_numbers is None:
        recipients = _split_csv(_env_text("SOLAPI_DEFAULT_RECIPIENTS", ""))

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
    if not _env_text("META_ACCESS_TOKEN") or not _env_text("META_INSTAGRAM_ID"):
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
    telegram_enabled = _resolve_channel_enabled(channel_overrides.get("telegram"), SEND_TELEGRAM_ENABLED)
    sms_enabled = _resolve_channel_enabled(channel_overrides.get("sms"), SEND_SMS_ENABLED)
    instagram_enabled = _resolve_channel_enabled(channel_overrides.get("instagram"), SEND_INSTAGRAM_ENABLED)

    results = {
        "telegram": _build_skipped_result("이번 실행 설정으로 텔레그램 발송을 건너뜁니다.")
        if not telegram_enabled
        else _build_result(False, "telegram_report 가 비어 있습니다."),
        "sms": _build_skipped_result("이번 실행 설정으로 SMS 발송을 건너뜁니다.")
        if not sms_enabled
        else _build_result(False, "alimtalk_message 또는 sms_message 가 비어 있습니다."),
        "instagram": _build_skipped_result("이번 실행 설정으로 인스타그램 업로드를 건너뜁니다.")
        if not instagram_enabled
        else _build_result(False, "instagram_caption 이 비어 있습니다."),
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
