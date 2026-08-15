# KB 주간 완전자동화 운영 기준

## 일정

- 금요일 10:00 Asia/Seoul: Codex desktop 예약 작업 `kb`
- 금요일 11:00 Asia/Seoul: macOS launchd Telegram 안전 발송

Codex desktop 작업이 정상적으로 Telegram message ID를 남기면 11시 작업은 중복 발송을 건너뜁니다. 데스크톱 작업이 실행되지 않았거나 Telegram 발송 영수증이 없으면 11시 작업이 외부 LLM API 없이 결정론적 리포트를 발송합니다.

## 채널 정책

| 채널 | 자동 실행 | 성공 조건 |
|---|---:|---|
| Telegram | 예 | 본문과 2개 첨부의 실제 message ID |
| Canva | 프로젝트 생성만 | 16페이지, 편집 URL, 6개 상승 섹션 검증 |
| Naver Blog | 초안만 | 로컬 Markdown 파일 |
| Instagram | 아니오 | 공개 업로드 금지 |
| SMS | 아니오 | 명시적 활성화 전까지 금지 |
| Kakao | 아니오 | 명시적 활성화 전까지 금지 |

## 영수증

- 데스크톱 최종 영수증: `reports/runtime/last_desktop_weekly_run.json`
- launchd 최종 영수증: `reports/runtime/last_scheduled_run.json`
- 실행별 이력: `reports/runtime/history/`

테스트 통과나 파일 생성은 운영 성공이 아닙니다. Telegram `message_ids`가 비어 있으면 전체 작업은 실패입니다.
