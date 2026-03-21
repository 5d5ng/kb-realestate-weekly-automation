# KB부동산 주간 자동화 파이프라인

KB부동산 주간시계열 데이터를 기반으로 지역별 흐름을 분석하고, 실거래/뉴스를 보강한 뒤, 콘텐츠 생성과 발송까지 이어지는 자동화 프로젝트입니다.

현재 기준으로 로컬 테스트와 주요 모듈 검증은 가능하며, 텔레그램과 SOLAPI SMS 발송도 실제 테스트가 완료된 상태입니다.

## 현재 구현 범위

- KB부동산 주간시계열 엑셀 자동 다운로드 및 분석
- 매매/전세 상하위 지역 추출
- 수도권/비수도권, 상위/하위 기준 8개 콘텐츠 버킷 선별
- KB 부동산 실거래 데이터 조회
- 지역별 `84`, `59` 타입 최근 실거래 조회
- 네이버 신문보기 기준 부동산 뉴스 수집
- 지정 언론사/키워드 기반 뉴스 중요도 필터
- 플랫폼별 콘텐츠 생성 구조 분리
- 인스타 카드뉴스/캡션/네이버 블로그용 프롬프트 파일 저장
- 텔레그램 실제 발송
- SOLAPI SMS 실제 발송
- APScheduler 기반 주간 실행 뼈대
- 로컬 CLI 테스트 스크립트
- 로컬 웹 실행 페이지

## 현재 기준 실제 채널 상태

- 텔레그램: 구현 및 실발송 확인 완료
- SOLAPI: SMS 기준 구현 및 실발송 확인 완료
- 카카오 알림톡: 아직 미구현, 현재는 SMS로 대체 운영
- 인스타그램 업로드: 계정 조건 미충족으로 보류
- 네이버 블로그 게시: 아직 미구현
- 프롬프트 파일 저장: 구현 완료

## 프로젝트 구조

```text
app.py                         Flask 웹 진입점
analyzer.py                    KB 주간시계열 분석
realestate.py                  KB 실거래 조회
news.py                        네이버 뉴스/신문보기 수집 및 필터링
reporter.py                    콘텐츠 생성 오케스트레이터
sender.py                      텔레그램/SMS 발송
scheduler.py                   파이프라인 실행 컨트롤러 + APScheduler

reporters/common.py            공통 프롬프트/LLM/파일 저장 유틸
reporters/telegram.py          텔레그램 리포트 생성
reporters/alimtalk.py          짧은 메시지 생성
reporters/instagram.py         인스타 캡션 생성
reporters/cardnews.py          카드뉴스 스크립트 생성
reporters/blog.py              네이버 블로그 프롬프트 생성

scripts/run_local_pipeline_test.py   로컬 CLI 테스트
scripts/run_local_web.py             로컬 웹 테스트 실행기

reports/prompts/               저장된 프롬프트 파일
reports/                       테스트 결과 파일
downloads/                     다운로드된 KB 파일
```

## 데이터 흐름

전체 흐름은 아래 순서입니다.

1. `analyzer.py`
   - KB 주간시계열 파일 다운로드
   - 매매/전세 시트 파싱
   - 최신 `current`, 전주 대비 `delta` 계산
   - 콘텐츠용 8개 지역 버킷 생성

2. `realestate.py`
   - `analysis` 결과를 받아 지역명 해석
   - KB 실거래 데이터 조회
   - 지역별 `84`, `59` 타입 최근 거래 정리

3. `news.py`
   - 네이버 언론사 신문보기에서 최근 기사 수집
   - 대상 언론사 필터
   - 제외 키워드 제거
   - 우선순위 키워드 점수화

4. `reporter.py`
   - 텔레그램/문자/인스타/카드뉴스용 콘텐츠 생성
   - 인스타 카드뉴스/캡션/네이버 블로그용 프롬프트 파일 저장

5. `sender.py`
   - 텔레그램 발송
   - SOLAPI SMS 발송
   - 인스타는 현재 보류

6. `scheduler.py`
   - 위 전체 흐름을 하나의 파이프라인으로 실행

## 환경 변수

기본 템플릿은 [`.env.example`](./.env.example) 에 있습니다.

시작 전:

```bash
cp .env.example .env
```

### 최소 테스트용

뉴스 수집과 발송 없이 로컬 dry-run 만 볼 경우:

- `NAVER_CLIENT_ID`
- `NAVER_CLIENT_SECRET`

### 콘텐츠 생성 실제 LLM 호출까지 테스트할 경우

- `OPENAI_API_KEY`
- `GEMINI_API_KEY`
- `ANTHROPIC_API_KEY` 선택

키가 없어도 템플릿 fallback 으로 동작합니다.

### 발송 테스트용

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SOLAPI_API_KEY`
- `SOLAPI_API_SECRET`
- `SOLAPI_SENDER`
- `SOLAPI_DEFAULT_RECIPIENTS`

### 보류 중

- `META_ACCESS_TOKEN`
- `META_INSTAGRAM_ID`

## 설치

```bash
pip install -r requirements.txt
```

## 로컬 테스트 방법

### 1. 가장 안전한 테스트: CLI dry-run

발송 없이 전체 흐름만 확인합니다.

```bash
python scripts/run_local_pipeline_test.py
```

기본값:

- 뉴스 기간: 1일
- 최대 뉴스: 3건
- 지역/타입별 실거래: 2건
- 발송: 생략

### 2. 조금 더 자세한 CLI 테스트

```bash
python scripts/run_local_pipeline_test.py \
  --news-days 1 \
  --news-max-articles 3 \
  --transaction-limit 2 \
  --output reports/local_test_result.json
```

이 실행은 결과 JSON 을 파일로 저장합니다.

### 3. 실제 발송 포함 테스트

주의: 실제 텔레그램/SMS가 발송됩니다.

```bash
python scripts/run_local_pipeline_test.py --send
```

### 4. 브라우저에서 편하게 테스트

로컬 웹 실행:

```bash
python scripts/run_local_web.py
```

브라우저 접속:

```text
http://127.0.0.1:5000
```

화면에서 할 수 있는 것:

- Dry Run 실행
- 실제 발송 실행
- 뉴스 기간/기사 수/실거래 건수 입력
- 실행 결과 JSON 바로 확인

### 5. 로컬에서 스케줄러까지 같이 확인

```bash
python scripts/run_local_web.py --with-scheduler
```

기본값은 수동 실행 전용이며, 이 옵션을 줘야 APScheduler 도 함께 켜집니다.

## API 엔드포인트

### `GET /`

로컬 실행 페이지

### `GET /health`

헬스체크

응답 예시:

```json
{
  "status": "ok"
}
```

### `POST /run`

수동 파이프라인 실행

요청 예시:

```json
{
  "send": false,
  "news_days": 1,
  "news_max_articles": 3,
  "transaction_limit": 2
}
```

응답에는 아래 요약이 포함됩니다.

- `analysis_summary`
- `transaction_summary`
- `news_summary`
- `contents_summary`
- `send_results`

## 생성 결과 확인 위치

### 프롬프트 파일

실행 후 아래 파일들이 저장됩니다.

- `reports/prompts/instagram_caption_prompt.txt`
- `reports/prompts/card_news_script_prompt.txt`
- `reports/prompts/naver_blog_post_prompt.txt`

### 실거래 테스트 리포트

- `reports/realestate_test_report.txt`

## 현재 모델 라우팅

현재 기본 설정은 다음과 같습니다.

- 텔레그램 리포트: `openai / gpt-5-mini`
- 알림 메시지: `none` 또는 템플릿
- 인스타 캡션: `gemini / gemini-2.5-flash-lite`
- 카드뉴스 스크립트: `openai / gpt-5-mini`
- 네이버 블로그 프롬프트: `none`

실제 설정값은 `.env` 로 변경할 수 있습니다.

## 현재 제한 사항

- `scheduler.py` 는 실제 실행 컨트롤러로 동작하지만, 아직 운영 배포 전 최종 통합 검증은 더 필요합니다.
- `app.py` 의 로컬 웹 실행기는 편의 기능 위주입니다.
- 카카오 알림톡은 아직 붙지 않았고, 현재는 SMS로 대체되어 있습니다.
- 인스타 업로드는 계정 조건 충족 후 구현 예정입니다.
- 네이버 블로그는 프롬프트 저장까지만 구현되어 있습니다.

## 권장 사용 순서

1. `.env` 설정
2. `pip install -r requirements.txt`
3. `python scripts/run_local_pipeline_test.py`
4. 결과 확인
5. 필요 시 `python scripts/run_local_web.py`
6. 마지막에 `--send` 로 실제 발송 테스트

## Railway 배포 메모

- 권장 시작 커맨드: `gunicorn app:app --bind 0.0.0.0:$PORT`
- 헬스체크 경로: `/health`
- 운영 권장 인스턴스 수: `1`
- 현재 실행 우선순위 정책: `수동 실행 > 예약 실행`
- `ENABLE_SCHEDULER=1` 이면 예약 실행 활성화, `0` 이면 웹/API 수동 실행만 사용
- Railway에 등록할 환경변수는 로컬 `.env` 기준으로 동일하게 옮기면 됩니다.

## 다음 개발 우선순위

- `app.py` 와 `scheduler.py` 최종 통합 검증
- 카카오 알림톡 실제 연동
- 인스타그램 업로드
- 네이버 블로그 실제 게시 기능
- Railway 배포 및 운영 안정화
