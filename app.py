from __future__ import annotations

import json
import os
import threading
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from flask import Flask, jsonify, render_template_string, request, send_file

from scheduler import init_scheduler, run_pipeline
from sender import SEND_INSTAGRAM_ENABLED, SEND_SMS_ENABLED, SEND_TELEGRAM_ENABLED
from valuation_web import valuation_bp

app = Flask(__name__)
app.register_blueprint(valuation_bp)
BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "reports" / "exports"
RUN_STATE_LOCK = threading.Lock()
RUN_STATE: dict[str, dict[str, Any]] = {}
MAX_RUN_HISTORY = 20
RUN_SEQUENCE = 0

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
    .channel-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .channel-item {
      display: flex;
      align-items: flex-start;
      gap: 10px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }
    .channel-item input[type="checkbox"] {
      width: auto;
      margin-top: 2px;
      accent-color: var(--accent);
    }
    .channel-copy {
      display: grid;
      gap: 4px;
    }
    .channel-title {
      font-size: 14px;
      font-weight: 700;
      color: var(--ink);
    }
    .channel-desc {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.5;
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
    .status-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-top: 22px;
    }
    .status-card {
      display: grid;
      gap: 12px;
    }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 10px;
    }
    .meta-item {
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }
    .meta-label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .meta-value {
      font-size: 15px;
      font-weight: 700;
      word-break: break-word;
    }
    .stage-board {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .stage-pill {
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      font-size: 13px;
      font-weight: 700;
    }
    .stage-pill.running {
      border-color: var(--accent);
      background: var(--accent-soft);
      color: var(--accent);
    }
    .stage-pill.completed {
      border-color: #15803d;
      background: #dcfce7;
      color: #166534;
    }
    .stage-pill.failed {
      border-color: #b91c1c;
      background: #fee2e2;
      color: #991b1b;
    }
    .stage-pill.skipped {
      border-color: #92400e;
      background: #ffedd5;
      color: #9a3412;
    }
    .download-row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    .download-link {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
      padding: 0 14px;
      border-radius: 12px;
      background: #1d4ed8;
      color: white;
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
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
      <p style="margin-top: 10px;">
        단지 상대가치 분석 서비스는 <code>/valuation</code> 에서 별도 화면으로 사용할 수 있습니다.
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
        <p>이번 실행에서 사용할 플랫폼을 직접 고를 수 있습니다. 체크한 채널만 발송됩니다.</p>

        <label for="send-news-days">뉴스 수집 기간</label>
        <input id="send-news-days" name="news_days" type="number" min="1" value="1">

        <label for="send-news-max">최대 뉴스 수</label>
        <input id="send-news-max" name="news_max_articles" type="number" min="1" value="3">

        <label for="send-transaction-limit">실거래 건수</label>
        <input id="send-transaction-limit" name="transaction_limit" type="number" min="1" value="2">

        <label>발송 플랫폼</label>
        <div class="channel-list">
          <label class="channel-item">
            <input
              type="checkbox"
              name="send_telegram"
              {% if default_channels.telegram %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">텔레그램</span>
              <span class="channel-desc">운영 기본 채널입니다. 텍스트 리포트를 바로 전송합니다.</span>
            </span>
          </label>

          <label class="channel-item">
            <input
              type="checkbox"
              name="send_sms"
              {% if default_channels.sms %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">SMS / 알림 메시지</span>
              <span class="channel-desc">솔라피를 통해 발송합니다. 비용이 발생할 수 있습니다.</span>
            </span>
          </label>

          <label class="channel-item">
            <input
              type="checkbox"
              name="send_instagram"
              {% if default_channels.instagram %}checked{% endif %}
            >
            <span class="channel-copy">
              <span class="channel-title">인스타그램</span>
              <span class="channel-desc">현재 업로드 기능은 보류 상태입니다. 켜도 스킵될 수 있습니다.</span>
            </span>
          </label>
        </div>

        <div class="actions">
          <button type="submit" class="secondary">실제 발송 실행</button>
        </div>
      </form>
    </section>

    <p class="hint">
      프롬프트 파일은 <code>reports/prompts/</code> 아래에 저장됩니다.
      수동 API 호출은 <code>POST /run</code> 으로도 가능합니다.
    </p>

    <section class="status-grid">
      <div class="card status-card">
        <h2>실행 상태</h2>
        <p>현재 실행 ID, 상태, 단계, 생성 파일 다운로드를 여기서 바로 확인합니다.</p>
        <div class="meta-grid">
          <div class="meta-item">
            <span class="meta-label">상태</span>
            <span class="meta-value" id="run-status">대기</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">현재 단계</span>
            <span class="meta-value" id="run-stage">-</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">Run ID</span>
            <span class="meta-value" id="run-id">-</span>
          </div>
          <div class="meta-item">
            <span class="meta-label">시작 시각</span>
            <span class="meta-value" id="run-started-at">-</span>
          </div>
        </div>
        <div class="stage-board" id="stage-board"></div>
        <div class="download-row">
          <a id="download-artifacts" class="download-link" href="#" hidden>생성 파일 ZIP 다운로드</a>
        </div>
      </div>
      <div class="card">
        <h2>진행 로그</h2>
        <p>현재 어떤 단계가 실행 중인지 순서대로 표시합니다.</p>
        <pre id="progress">아직 실행 로그가 없습니다.</pre>
      </div>
      <div class="card">
        <h2>최종 결과</h2>
        <p>실행이 끝나면 최종 응답 JSON 이 여기에 표시됩니다.</p>
        <pre id="result">아직 실행 결과가 없습니다.</pre>
      </div>
    </section>
  </div>

  <script>
    let activeRunId = null;
    let activePoller = null;
    const STAGES = ["queued", "lock", "analysis", "transactions", "news", "contents", "send", "done"];

    function updateRunMeta(payload) {
      document.getElementById("run-status").textContent = payload.status || "대기";
      document.getElementById("run-stage").textContent = payload.current_stage || "-";
      document.getElementById("run-id").textContent = payload.run_id || "-";
      document.getElementById("run-started-at").textContent = payload.started_at || "-";

      const downloadLink = document.getElementById("download-artifacts");
      if (payload.artifact_download_url && (payload.status === "completed" || payload.status === "failed" || payload.status === "skipped")) {
        downloadLink.href = payload.artifact_download_url;
        downloadLink.hidden = false;
      } else {
        downloadLink.hidden = true;
        downloadLink.removeAttribute("href");
      }
    }

    function renderStageBoard(payload) {
      const board = document.getElementById("stage-board");
      const logs = payload.logs || [];
      const stageStatuses = {};

      for (const log of logs) {
        stageStatuses[log.stage] = log.status || "running";
      }

      if (payload.status === "failed" && payload.current_stage) {
        stageStatuses[payload.current_stage] = "failed";
      }
      if (payload.status === "skipped" && payload.current_stage) {
        stageStatuses[payload.current_stage] = "skipped";
      }
      if (payload.status === "completed") {
        stageStatuses["done"] = "completed";
      }

      board.innerHTML = STAGES.map((stage) => {
        const status = stageStatuses[stage] || "";
        const labelMap = {
          queued: "대기",
          lock: "락",
          analysis: "분석",
          transactions: "실거래",
          news: "뉴스",
          contents: "콘텐츠",
          send: "발송",
          done: "완료",
        };
        return `<div class="stage-pill ${status}">${labelMap[stage]}</div>`;
      }).join("");
    }

    function buildPayload(form, send) {
      const formData = new FormData(form);
      const data = Object.fromEntries(formData.entries());
      data.send = send;

      if (send) {
        data.send_telegram = form.querySelector('input[name="send_telegram"]')?.checked || false;
        data.send_sms = form.querySelector('input[name="send_sms"]')?.checked || false;
        data.send_instagram = form.querySelector('input[name="send_instagram"]')?.checked || false;
      }

      return data;
    }

    function renderProgress(payload) {
      const progressEl = document.getElementById("progress");
      const logs = payload.logs || [];
      if (!logs.length) {
        progressEl.textContent = "아직 실행 로그가 없습니다.";
        return;
      }

      const lines = logs.map((log) => {
        const extra = log.extra ? ` | ${JSON.stringify(log.extra)}` : "";
        return `[${log.time}] [${log.stage}] ${log.message}${extra}`;
      });
      progressEl.textContent = lines.join("\\n");
      progressEl.scrollTop = progressEl.scrollHeight;
    }

    function renderResult(payload) {
      const resultEl = document.getElementById("result");
      if (payload.result) {
        resultEl.textContent = JSON.stringify(payload.result, null, 2);
        return;
      }
      if (payload.error) {
        resultEl.textContent = JSON.stringify(payload.error, null, 2);
        return;
      }
      resultEl.textContent = JSON.stringify(payload, null, 2);
    }

    async function pollRun(runId) {
      const progressEl = document.getElementById("progress");
      const resultEl = document.getElementById("result");

      try {
        const response = await fetch(`/run/status/${runId}`);
        const payload = await response.json();
        updateRunMeta(payload);
        renderStageBoard(payload);
        renderProgress(payload);

        if (payload.status === "completed" || payload.status === "failed" || payload.status === "skipped") {
          renderResult(payload);
          if (activePoller) {
            clearInterval(activePoller);
            activePoller = null;
          }
          activeRunId = null;
          return;
        }

        resultEl.textContent = JSON.stringify(
          {
            run_id: payload.run_id,
            status: payload.status,
            started_at: payload.started_at,
            current_stage: payload.current_stage,
          },
          null,
          2
        );
      } catch (error) {
        progressEl.textContent += `\\n[error] 상태 조회 실패: ${String(error)}`;
        if (activePoller) {
          clearInterval(activePoller);
          activePoller = null;
        }
        activeRunId = null;
      }
    }

    async function submitForm(form, send) {
      const progressEl = document.getElementById("progress");
      const resultEl = document.getElementById("result");
      const data = buildPayload(form, send);

      if (activePoller) {
        clearInterval(activePoller);
        activePoller = null;
      }
      activeRunId = null;
      updateRunMeta({ status: "queued", current_stage: "queued", run_id: "-", started_at: "-" });
      renderStageBoard({ logs: [{ stage: "queued", status: "running" }], status: "queued" });
      progressEl.textContent = "실행 요청을 전송했습니다.";
      resultEl.textContent = "백그라운드 실행을 시작하는 중입니다.";

      try {
        const response = await fetch("/run/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(data),
        });
        const payload = await response.json();
        if (!response.ok || !payload.run_id) {
          resultEl.textContent = JSON.stringify(payload, null, 2);
          return;
        }

        activeRunId = payload.run_id;
        progressEl.textContent = `[${payload.started_at}] [init] 실행 요청 등록\\nrun_id=${payload.run_id}`;
        updateRunMeta({
          status: payload.status,
          current_stage: "queued",
          run_id: payload.run_id,
          started_at: payload.started_at,
        });
        renderStageBoard({ logs: [{ stage: "queued", status: "running" }], status: "queued" });
        resultEl.textContent = JSON.stringify(
          {
            run_id: payload.run_id,
            status: payload.status,
            detail: payload.detail,
          },
          null,
          2
        );

        await pollRun(payload.run_id);
        activePoller = setInterval(() => {
          if (!activeRunId) {
            clearInterval(activePoller);
            activePoller = null;
            return;
          }
          pollRun(activeRunId);
        }, 1500);
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
      const ok = window.confirm("체크한 플랫폼으로 실제 발송을 진행할까요?");
      if (!ok) return;
      submitForm(event.currentTarget, true);
    });

    updateRunMeta({ status: "대기", current_stage: "-", run_id: "-", started_at: "-" });
    renderStageBoard({});
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


def _parse_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    send = _parse_bool(payload.get("send"), default=False)
    news_days = _parse_int(payload.get("news_days"), default=1)
    news_max_articles = _parse_int(payload.get("news_max_articles"), default=3)
    transaction_limit = _parse_int(payload.get("transaction_limit"), default=2)
    channel_overrides = {
        "telegram": _parse_bool(payload.get("send_telegram"), default=False),
        "sms": _parse_bool(payload.get("send_sms"), default=False),
        "instagram": _parse_bool(payload.get("send_instagram"), default=False),
    }
    return {
        "send": send,
        "news_days": news_days,
        "news_max_articles": news_max_articles,
        "transaction_limit": transaction_limit,
        "channel_overrides": channel_overrides if send else None,
    }


def _trim_run_history() -> None:
    if len(RUN_STATE) <= MAX_RUN_HISTORY:
        return
    sorted_items = sorted(RUN_STATE.items(), key=lambda item: item[1].get("created_order", 0))
    for run_id, _state in sorted_items[:-MAX_RUN_HISTORY]:
        RUN_STATE.pop(run_id, None)


def _to_relative_name(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return path.name


def _write_json_to_zip(zip_handle: zipfile.ZipFile, arcname: str, payload: dict[str, Any]) -> None:
    zip_handle.writestr(
        arcname,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def _create_artifact_bundle(run_id: str, state: dict[str, Any], result: dict[str, Any]) -> str | None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    bundle_path = EXPORT_DIR / f"{run_id}_artifacts.zip"
    artifact_files = result.get("artifact_files", []) or []
    existing_paths = [Path(path) for path in artifact_files if Path(path).exists()]
    written_arcnames: set[str] = set()

    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        _write_json_to_zip(
            zf,
            "run/result.json",
            result,
        )
        _write_json_to_zip(
            zf,
            "run/logs.json",
            {"logs": state.get("logs", [])},
        )

        for path in existing_paths:
            if path.is_dir():
                for file_path in sorted(child for child in path.rglob("*") if child.is_file()):
                    arcname = _to_relative_name(file_path)
                    if arcname in written_arcnames:
                        continue
                    written_arcnames.add(arcname)
                    zf.write(file_path, arcname=arcname)
            elif path.is_file():
                arcname = _to_relative_name(path)
                if arcname in written_arcnames:
                    continue
                written_arcnames.add(arcname)
                zf.write(path, arcname=arcname)

    return str(bundle_path)


def _append_run_log(run_id: str, event: dict[str, Any]) -> None:
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return
        state["logs"].append(event)
        state["current_stage"] = event.get("stage", state.get("current_stage"))
        state["updated_at"] = event.get("time")


def _execute_run_async(run_id: str, options: dict[str, Any]) -> None:
    def progress_callback(event: dict[str, Any]) -> None:
        _append_run_log(run_id, event)

    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return
        state["status"] = "running"

    result = run_pipeline(
        send=options["send"],
        trigger="manual",
        news_days=options["news_days"],
        news_max_articles=options["news_max_articles"],
        transaction_limit=options["transaction_limit"],
        channel_overrides=options["channel_overrides"],
        progress_callback=progress_callback,
    )

    final_status = "completed" if result.get("success") and not result.get("skipped") else "failed"
    if result.get("skipped"):
        final_status = "skipped"

    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return
        artifact_bundle_path: str | None = None
        artifact_bundle_error: str | None = None
        try:
            artifact_bundle_path = _create_artifact_bundle(run_id, state, result)
        except Exception as exc:
            artifact_bundle_error = str(exc)
        state["status"] = final_status
        state["result"] = result
        state["updated_at"] = result.get("completed_at")
        state["artifact_bundle_path"] = artifact_bundle_path
        state["artifact_download_url"] = f"/run/artifacts/{run_id}"
        state["artifact_bundle_error"] = artifact_bundle_error


def _start_background_run(options: dict[str, Any]) -> dict[str, Any]:
    global RUN_SEQUENCE
    run_id = uuid4().hex
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with RUN_STATE_LOCK:
        RUN_SEQUENCE += 1
        RUN_STATE[run_id] = {
            "run_id": run_id,
            "status": "queued",
            "started_at": started_at,
            "updated_at": started_at,
            "created_order": RUN_SEQUENCE,
            "current_stage": "queued",
            "options": options,
            "logs": [
                {
                    "time": started_at,
                    "stage": "queued",
                    "status": "queued",
                    "message": "실행 요청이 등록되었습니다.",
                    "extra": {
                        "send": options["send"],
                        "channel_overrides": options.get("channel_overrides") or {},
                    },
                }
            ],
        }
        _trim_run_history()

    thread = threading.Thread(target=_execute_run_async, args=(run_id, options), daemon=True)
    thread.start()
    return {
        "success": True,
        "run_id": run_id,
        "status": "queued",
        "started_at": started_at,
        "detail": "백그라운드 실행을 시작했습니다.",
    }


@app.route("/")
def index():
    return render_template_string(
        INDEX_TEMPLATE,
        default_channels={
            "telegram": SEND_TELEGRAM_ENABLED,
            "sms": SEND_SMS_ENABLED,
            "instagram": SEND_INSTAGRAM_ENABLED,
        },
    )


@app.route("/run", methods=["POST"])
def run_manual():
    """수동 실행 엔드포인트"""
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    options = _parse_run_payload(payload)

    result = run_pipeline(
        send=options["send"],
        trigger="manual",
        news_days=options["news_days"],
        news_max_articles=options["news_max_articles"],
        transaction_limit=options["transaction_limit"],
        channel_overrides=options["channel_overrides"],
    )
    status_code = 200 if result.get("success") else 500
    return jsonify(result), status_code


@app.route("/run/start", methods=["POST"])
def run_manual_start():
    payload = request.get_json(silent=True) or request.form.to_dict() or {}
    options = _parse_run_payload(payload)
    return jsonify(_start_background_run(options)), 202


@app.route("/run/status/<run_id>", methods=["GET"])
def run_status(run_id: str):
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return jsonify({"success": False, "error": "run_id 를 찾을 수 없습니다."}), 404
        return jsonify(state)


@app.route("/run/artifacts/<run_id>", methods=["GET"])
def download_artifacts(run_id: str):
    with RUN_STATE_LOCK:
        state = RUN_STATE.get(run_id)
        if not state:
            return jsonify({"success": False, "error": "run_id 를 찾을 수 없습니다."}), 404
        bundle_path = state.get("artifact_bundle_path")

    if not bundle_path:
        return jsonify({"success": False, "error": "아직 다운로드 가능한 아티팩트가 없습니다."}), 404

    path = Path(bundle_path)
    if not path.exists():
        return jsonify({"success": False, "error": "아티팩트 파일을 찾을 수 없습니다."}), 404

    return send_file(path, as_attachment=True, download_name=path.name)


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
