"""
채널별 콘텐츠 발송
- 텔레그램
- 솔라피 SMS (비용 이슈로 기본 비활성화)
- 인스타그램 (별도 instagram-content-publisher MCP로 이동)
"""
from __future__ import annotations

import os
from pathlib import Path
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
        "telegram": {
            **_telegram_config_status(),
            "prompt_files_enabled": SEND_TELEGRAM_PROMPT_FILES_ENABLED,
        },
        "sms": {
            "api_key_present": bool(_env_text("SOLAPI_API_KEY")),
            "api_secret_present": bool(_env_text("SOLAPI_API_SECRET")),
            "sender_present": bool(_env_text("SOLAPI_SENDER")),
            "recipients_present": bool(_split_csv(_env_text("SOLAPI_DEFAULT_RECIPIENTS", ""))),
        },
        "instagram": {
            "publisher_mcp": "instagram-content-publisher",
            "access_token_present": bool(
                _env_text("INSTAGRAM_ACCESS_TOKEN") or _env_text("META_ACCESS_TOKEN")
            ),
            "instagram_id_present": bool(
                _env_text("INSTAGRAM_ACCOUNT_ID") or _env_text("META_INSTAGRAM_ID")
            ),
            "publishing_enabled": _env_flag("INSTAGRAM_PUBLISHING_ENABLED", False),
        },
        "kakao": {
            "rest_api_key_present": bool(_env_text("KAKAO_REST_API_KEY")),
            "tokens_stored": _kakao_tokens_present(),
        },
    }


def _kakao_tokens_present() -> bool:
    try:
        from valuation_db import get_json_cache
        return get_json_cache("kakao:refresh_token") is not None
    except Exception:
        return False


SEND_TELEGRAM_ENABLED = _env_flag("SEND_TELEGRAM_ENABLED", True)
SEND_TELEGRAM_PROMPT_FILES_ENABLED = _env_flag("SEND_TELEGRAM_PROMPT_FILES_ENABLED", True)
SEND_SMS_ENABLED = _env_flag("SEND_SMS_ENABLED", False)
SEND_INSTAGRAM_ENABLED = _env_flag("SEND_INSTAGRAM_ENABLED", False)
SEND_KAKAO_ENABLED = _env_flag("SEND_KAKAO_ENABLED", False)
MAX_TELEGRAM_MESSAGE_LEN = 3900
MAX_KAKAO_MESSAGE_LEN = 3800


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


def _split_telegram_message(message: str, limit: int = MAX_TELEGRAM_MESSAGE_LEN) -> list[str]:
    text = str(message or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    base_limit = max(500, limit - 16)
    paragraphs = [paragraph.strip() for paragraph in text.split("\n\n") if paragraph.strip()]
    chunks: list[str] = []
    current = ""

    def flush_current() -> None:
        nonlocal current
        if current:
            chunks.append(current.strip())
            current = ""

    for paragraph in paragraphs:
        candidate = f"{current}\n\n{paragraph}" if current else paragraph
        if len(candidate) <= base_limit:
            current = candidate
            continue

        flush_current()
        remaining = paragraph
        while remaining:
            if len(remaining) <= base_limit:
                current = remaining
                break

            split_at = remaining.rfind("\n", 0, base_limit)
            if split_at < int(base_limit * 0.5):
                split_at = remaining.rfind(" ", 0, base_limit)
            if split_at < int(base_limit * 0.5):
                split_at = base_limit

            chunks.append(remaining[:split_at].strip())
            remaining = remaining[split_at:].strip()

    flush_current()

    if len(chunks) <= 1:
        return chunks

    total = len(chunks)
    return [f"[{index + 1}/{total}]\n{chunk}" for index, chunk in enumerate(chunks)]


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

    chunks = _split_telegram_message(message)
    if not chunks:
        return _build_result(False, "텔레그램으로 보낼 메시지가 비어 있습니다.")

    message_ids: list[int] = []
    result: dict[str, Any] = {}

    for chunk_index, chunk in enumerate(chunks, start=1):
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                },
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # pragma: no cover - external API
            return _build_result(
                False,
                f"텔레그램 발송 실패 ({chunk_index}/{len(chunks)}): {exc}",
                sent_chunks=len(message_ids),
                total_chunks=len(chunks),
                message_ids=message_ids,
            )

        if not payload.get("ok"):
            return _build_result(
                False,
                f"텔레그램 API 응답이 실패로 반환되었습니다. ({chunk_index}/{len(chunks)})",
                response=payload,
                sent_chunks=len(message_ids),
                total_chunks=len(chunks),
                message_ids=message_ids,
            )

        result = payload.get("result", {})
        message_id = result.get("message_id")
        if isinstance(message_id, int):
            message_ids.append(message_id)

    return _build_result(
        True,
        "텔레그램 발송 성공",
        message_id=result.get("message_id"),
        message_ids=message_ids,
        chunk_count=len(chunks),
        characters=len(message),
        chat_id=result.get("chat", {}).get("id"),
    )


def send_telegram_document(
    file_path: str | Path,
    *,
    caption: str | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    """텔레그램 문서 파일 전송"""
    if not _resolve_channel_enabled(enabled, SEND_TELEGRAM_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 텔레그램 문서 전송을 건너뜁니다.")

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

    path = Path(file_path)
    if not path.is_file():
        return _build_result(False, f"텔레그램 문서 파일을 찾을 수 없습니다: {path}")

    try:
        with path.open("rb") as file_handle:
            response = requests.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={
                    "chat_id": chat_id,
                    "caption": caption or path.name,
                    "disable_content_type_detection": True,
                },
                files={"document": (path.name, file_handle, "text/plain")},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:  # pragma: no cover - external API
        return _build_result(False, f"텔레그램 문서 전송 실패: {exc}", path=str(path))

    if not payload.get("ok"):
        return _build_result(False, "텔레그램 문서 API 응답이 실패로 반환되었습니다.", path=str(path), response=payload)

    result = payload.get("result", {})
    return _build_result(
        True,
        "텔레그램 문서 전송 성공",
        path=str(path),
        filename=path.name,
        message_id=result.get("message_id"),
        chat_id=result.get("chat", {}).get("id"),
    )


def send_telegram_documents(
    file_paths: dict[str, str] | list[str] | tuple[str, ...],
    *,
    enabled: bool | None = None,
) -> dict[str, Any]:
    """프롬프트 등 여러 파일을 텔레그램 문서로 순차 전송"""
    if not _resolve_channel_enabled(enabled, SEND_TELEGRAM_PROMPT_FILES_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 텔레그램 프롬프트 파일 전송을 건너뜁니다.")

    if isinstance(file_paths, dict):
        items = [(str(label), str(path)) for label, path in file_paths.items() if path]
    else:
        items = [(Path(path).stem, str(path)) for path in file_paths if path]

    deduped_items: list[tuple[str, str]] = []
    seen_paths: set[str] = set()
    for label, path in items:
        if path in seen_paths:
            continue
        seen_paths.add(path)
        deduped_items.append((label, path))

    if not deduped_items:
        return _build_skipped_result("전송할 프롬프트 파일이 없습니다.")

    documents: list[dict[str, Any]] = []
    for label, path in deduped_items:
        result = send_telegram_document(
            path,
            caption=f"KB부동산 프롬프트: {label}",
            enabled=True,
        )
        result["task_name"] = label
        documents.append(result)

    failed = [item for item in documents if not item.get("success")]
    if failed:
        return _build_result(
            False,
            f"텔레그램 프롬프트 파일 {len(failed)}개 전송 실패",
            total=len(documents),
            sent=len(documents) - len(failed),
            failed=len(failed),
            documents=documents,
        )

    return _build_result(
        True,
        f"텔레그램 프롬프트 파일 {len(documents)}개 전송 성공",
        total=len(documents),
        sent=len(documents),
        failed=0,
        message_ids=[item.get("message_id") for item in documents if item.get("message_id")],
        documents=documents,
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


def _split_kakao_message(message: str, limit: int = MAX_KAKAO_MESSAGE_LEN) -> list[str]:
    text = str(message or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    base_limit = max(500, limit - 16)
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    current = ""

    def flush() -> None:
        nonlocal current
        if current:
            chunks.append(current.strip())
            current = ""

    for paragraph in paragraphs:
        candidate = f"{current}\n\n{paragraph}" if current else paragraph
        if len(candidate) <= base_limit:
            current = candidate
            continue
        flush()
        remaining = paragraph
        while remaining:
            if len(remaining) <= base_limit:
                current = remaining
                break
            split_at = remaining.rfind("\n", 0, base_limit)
            if split_at < int(base_limit * 0.5):
                split_at = remaining.rfind(" ", 0, base_limit)
            if split_at < int(base_limit * 0.5):
                split_at = base_limit
            chunks.append(remaining[:split_at].strip())
            remaining = remaining[split_at:].strip()

    flush()

    if len(chunks) <= 1:
        return chunks
    total = len(chunks)
    return [f"[{i + 1}/{total}]\n{chunk}" for i, chunk in enumerate(chunks)]


def _send_kakao_token_alert(error_detail: str) -> None:
    alert = (
        "[KB자동화 알림] 카카오톡 토큰이 만료되었습니다.\n"
        f"오류: {error_detail}\n"
        "scripts/kakao_auth.py 를 다시 실행하여 토큰을 갱신해 주세요."
    )
    try:
        send_telegram(alert, enabled=True)
    except Exception:
        pass


def send_kakao(message: str, enabled: bool | None = None) -> dict[str, Any]:
    """카카오톡 나에게 보내기"""
    if not _resolve_channel_enabled(enabled, SEND_KAKAO_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 카카오톡 발송을 건너뜁니다.")

    if not _env_text("KAKAO_REST_API_KEY"):
        return _build_result(False, "KAKAO_REST_API_KEY 환경변수가 비어 있습니다.")

    try:
        from kakao_token import KakaoTokenError, get_valid_access_token
        access_token = get_valid_access_token()
    except KakaoTokenError as exc:
        _send_kakao_token_alert(str(exc))
        return _build_result(False, f"카카오 토큰 갱신 실패: {exc}")
    except Exception as exc:
        return _build_result(False, f"카카오 토큰 조회 실패: {exc}")

    import json as _json

    chunks = _split_kakao_message(message)
    if not chunks:
        return _build_result(False, "카카오톡으로 보낼 메시지가 비어 있습니다.")

    send_count = 0
    for chunk_index, chunk in enumerate(chunks, start=1):
        template_object = {
            "object_type": "text",
            "text": chunk,
            "link": {
                "web_url": "https://kbland.kr",
                "mobile_web_url": "https://kbland.kr",
            },
            "button_title": "KB부동산 바로가기",
        }
        try:
            resp = requests.post(
                "https://kapi.kakao.com/v2/api/talk/memo/default/send",
                headers={"Authorization": f"Bearer {access_token}"},
                data={"template_object": _json.dumps(template_object, ensure_ascii=False)},
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            return _build_result(
                False,
                f"카카오톡 나에게 보내기 실패 ({chunk_index}/{len(chunks)}): {exc}",
                sent_chunks=send_count,
                total_chunks=len(chunks),
            )

        if payload.get("result_code") != 0:
            return _build_result(
                False,
                f"카카오톡 API 응답 실패 ({chunk_index}/{len(chunks)}), result_code={payload.get('result_code')}",
                response=payload,
                sent_chunks=send_count,
                total_chunks=len(chunks),
            )
        send_count += 1

    return _build_result(
        True,
        "카카오톡 나에게 보내기 성공",
        chunk_count=len(chunks),
        characters=len(message),
    )


def post_instagram(
    caption: str,
    image_url: str | None = None,
    enabled: bool | None = None,
) -> dict[str, Any]:
    """기존 파이프라인 호환 함수. 실제 게시는 독립 Publisher MCP가 담당한다."""
    if not _resolve_channel_enabled(enabled, SEND_INSTAGRAM_ENABLED):
        return _build_skipped_result("이번 실행 설정으로 인스타그램 업로드를 건너뜁니다.")
    return _build_result(
        False,
        (
            "인스타그램 실제 게시는 독립 instagram-content-publisher MCP로 이동했습니다. "
            "content package 생성, 게시 계획, 해시 승인 순서로 실행하세요."
        ),
        migrated_to="mcp_servers/instagram_publisher_server.py",
        image_url=image_url,
        caption=caption,
    )


def send_all(
    contents: dict,
    phone_numbers: list[str] | None = None,
    sender_number: str | None = None,
    image_url: str | None = None,
    channel_overrides: dict[str, bool] | None = None,
    send_prompt_files: bool | None = None,
) -> dict[str, Any]:
    """전체 채널 발송 및 결과 반환"""
    telegram_message = contents.get("telegram_report")
    sms_message = contents.get("sms_message") or contents.get("alimtalk_message")
    instagram_caption = contents.get("instagram_caption")
    kakao_message = contents.get("alimtalk_message")
    channel_overrides = channel_overrides or {}
    telegram_enabled = _resolve_channel_enabled(channel_overrides.get("telegram"), SEND_TELEGRAM_ENABLED)
    prompt_files_enabled = telegram_enabled and _resolve_channel_enabled(
        send_prompt_files,
        SEND_TELEGRAM_PROMPT_FILES_ENABLED,
    )
    sms_enabled = _resolve_channel_enabled(channel_overrides.get("sms"), SEND_SMS_ENABLED)
    instagram_enabled = _resolve_channel_enabled(channel_overrides.get("instagram"), SEND_INSTAGRAM_ENABLED)
    kakao_enabled = _resolve_channel_enabled(channel_overrides.get("kakao"), SEND_KAKAO_ENABLED)

    results = {
        "telegram": _build_skipped_result("이번 실행 설정으로 텔레그램 발송을 건너뜁니다.")
        if not telegram_enabled
        else _build_result(False, "telegram_report 가 비어 있습니다."),
        "telegram_prompt_files": _build_skipped_result("이번 실행 설정으로 텔레그램 프롬프트 파일 전송을 건너뜁니다.")
        if not prompt_files_enabled
        else _build_result(False, "전송할 프롬프트 파일이 비어 있습니다."),
        "sms": _build_skipped_result("이번 실행 설정으로 SMS 발송을 건너뜁니다.")
        if not sms_enabled
        else _build_result(False, "alimtalk_message 또는 sms_message 가 비어 있습니다."),
        "instagram": _build_skipped_result("이번 실행 설정으로 인스타그램 업로드를 건너뜁니다.")
        if not instagram_enabled
        else _build_result(False, "instagram_caption 이 비어 있습니다."),
        "kakao": _build_skipped_result("이번 실행 설정으로 카카오톡 발송을 건너뜁니다.")
        if not kakao_enabled
        else _build_result(False, "alimtalk_message 가 비어 있습니다."),
    }

    if telegram_message:
        results["telegram"] = send_telegram(
            telegram_message,
            enabled=channel_overrides.get("telegram"),
        )
    if prompt_files_enabled:
        results["telegram_prompt_files"] = send_telegram_documents(
            contents.get("prompt_files", {}) or {},
            enabled=True,
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
    if kakao_message:
        results["kakao"] = send_kakao(
            message=kakao_message,
            enabled=channel_overrides.get("kakao"),
        )

    return results


if __name__ == "__main__":
    from pprint import pprint

    pprint(
        {
            "solapi_balance": get_solapi_balance(),
            "telegram": send_telegram("[KB자동화 테스트] sender.py 연결 확인"),
            "sms": send_sms("[KB자동화 테스트] SOLAPI SMS 연결 확인"),
            "kakao": send_kakao("[KB자동화 테스트] 카카오톡 나에게 보내기 연결 확인"),
        }
    )
