from __future__ import annotations

import os

from flask import Flask, jsonify, render_template_string, request

from scheduler import init_scheduler, run_pipeline

app = Flask(__name__)

INDEX_TEMPLATE = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KB부동산 자동화 로컬 실행기</title>
  <style>
    :root {
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #1e1e1e;
      --muted: #6d665c;
      --line: #d7cdbb;
      --accent: #0f766e;
      --accent-soft: #d8f3ef;
      --warn: #7c2d12;
      --warn-soft: #ffedd5;
    }
    body {
      margin: 0;
      font-family: "Pretendard", "Apple SD Gothic Neo", sans-serif;
      background: linear-gradient(180deg, #f8f5ed 0%, #efe5d2 100%);
      color: var(--ink);
    }
    .wrap {
      max-width: 920px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero {
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: var(--card);
      box-shadow: 0 14px 40px rgba(80, 62, 32, 0.08);
    }
    h1, h2 {
      margin: 0 0 12px;
    }
    p {
      margin: 0;
      line-height: 1.6;
      color: var(--muted);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-top: 22px;
    }
    .card {
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: var(--card);
      box-shadow: 0 10px 24px rgba(80, 62, 32, 0.06);
    }
    .badge {
      display: inline-block;
      margin-bottom: 10px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 13px;
      font-weight: 700;
    }
    .badge.warn {
      background: var(--warn-soft);
      color: var(--warn);
    }
    label {
      display: block;
      margin-top: 14px;
      margin-bottom: 6px;
      font-size: 14px;
      font-weight: 700;
    }
    input {
      width: 100%;
      box-sizing: border-box;
      padding: 12px 13px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
      font-size: 14px;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 18px;
    }
    button {
      border: 0;
      border-radius: 12px;
      padding: 12px 16px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      background: var(--accent);
      color: white;
    }
    button.secondary {
      background: #374151;
    }
    code {
      font-family: "SFMono-Regular", "Menlo", monospace;
      background: #f5f5f5;
      padding: 2px 6px;
      border-radius: 6px;
    }
    pre {
      margin-top: 22px;
      padding: 18px;
      border-radius: 18px;
      background: #111827;
      color: #f9fafb;
      overflow: auto;
      min-height: 220px;
      line-height: 1.5;
      font-size: 13px;
    }
    .hint {
      margin-top: 16px;
      font-size: 13px;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>KB부동산 자동화 로컬 실행기</h1>
      <p>
        브라우저에서 바로 <code>dry-run</code> 또는 실제 발송 테스트를 실행할 수 있습니다.
        처음에는 dry-run 으로 돌리고, 결과가 괜찮으면 실제 발송을 권장합니다.
      </p>
    </section>

    <section class="grid">
      <form class="card" id="dry-run-form">
        <span class="badge">추천</span>
        <h2>Dry Run</h2>
        <p>발송 없이 분석, 실거래, 뉴스, 콘텐츠 생성, 프롬프트 파일 저장까지만 점검합니다.</p>

        <label for="dry-news-days">뉴스 수집 기간</label>
        <input id="dry-news-days" name="news_days" type="number" min="1" value="1">

        <label for="dry-news-max">최대 뉴스 수</label>
        <input id="dry-news-max" name="news_max_articles" type="number" min="1" value="3">

        <label for="dry-transaction-limit">실거래 건수</label>
        <input id="dry-transaction-limit" name="transaction_limit" type="number" min="1" value="2">

        <div class="actions">
          <button type="submit">Dry Run 실행</button>
        </div>
      </form>

      <form class="card" id="send-form">
        <span class="badge warn">주의</span>
        <h2>실제 발송</h2>
        <p>텔레그램과 SOLAPI SMS 가 실제 발송됩니다. 수신 설정을 다시 확인한 뒤 실행하세요.</p>

        <label for="send-news-days">뉴스 수집 기간</label>
        <input id="send-news-days" name="news_days" type="number" min="1" value="1">

        <label for="send-news-max">최대 뉴스 수</label>
        <input id="send-news-max" name="news_max_articles" type="number" min="1" value="3">

        <label for="send-transaction-limit">실거래 건수</label>
        <input id="send-transaction-limit" name="transaction_limit" type="number" min="1" value="2">

        <div class="actions">
          <button type="submit" class="secondary">실제 발송 실행</button>
        </div>
      </form>
    </section>

    <p class="hint">
      프롬프트 파일은 <code>reports/prompts/</code> 아래에 저장됩니다.
      수동 API 호출은 <code>POST /run</code> 으로도 가능합니다.
    </p>

    <pre id="result">아직 실행 결과가 없습니다.</pre>
  </div>

  <script>
    async function submitForm(form, send) {
      const resultEl = document.getElementById("result");
      const data = Object.fromEntries(new FormData(form).entries());
      data.send = send;

      resultEl.textContent = "실행 중입니다. 외부 API 호출 때문에 1~수 분 정도 걸릴 수 있습니다.";

      try {
        const response = await fetch("/run", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(data),
        });
        const payload = await response.json();
        resultEl.textContent = JSON.stringify(payload, null, 2);
      } catch (error) {
        resultEl.textContent = JSON.stringify(
          { success: false, error: String(error) },
          null,
          2
        );
      }
    }

    document.getElementById("dry-run-form").addEventListener("submit", function (event) {
      event.preventDefault();
      submitForm(event.currentTarget, false);
    });

    document.getElementById("send-form").addEventListener("submit", function (event) {
      event.preventDefault();
      const ok = window.confirm("실제 텔레그램/SMS 발송을 진행할까요?");
      if (!ok) return;
      submitForm(event.currentTarget, true);
    });
  </script>
</body>
</html>
"""


def _parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "y", "yes", "on"}


def _parse_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@app.route("/")
def index():
    return render_template_string(INDEX_TEMPLATE)


@app.route("/run", methods=["POST"])
def run_manual():
    """수동 실행 엔드포인트"""
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    send = _parse_bool(payload.get("send"), default=False)
    news_days = _parse_int(payload.get("news_days"), default=1)
    news_max_articles = _parse_int(payload.get("news_max_articles"), default=3)
    transaction_limit = _parse_int(payload.get("transaction_limit"), default=2)

    result = run_pipeline(
        send=send,
        trigger="manual",
        news_days=news_days,
        news_max_articles=news_max_articles,
        transaction_limit=transaction_limit,
    )
    status_code = 200 if result.get("success") else 500
    return jsonify(result), status_code


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


def _should_enable_scheduler() -> bool:
    return os.getenv("ENABLE_SCHEDULER", "1").strip().lower() not in {"0", "false", "no", "off"}


if __name__ == "__main__":
    if _should_enable_scheduler():
        init_scheduler()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
