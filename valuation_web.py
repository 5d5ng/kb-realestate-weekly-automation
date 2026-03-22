from __future__ import annotations

from typing import Any

from flask import Blueprint, jsonify, render_template_string, request

import valuation_db
import valuation_service

valuation_bp = Blueprint("valuation", __name__, url_prefix="/valuation")

VALUATION_TEMPLATE = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KB 단지 상대가치 분석</title>
  <style>
    :root {
      --bg: #f3efe7;
      --card: #fffdfa;
      --ink: #1e1e1e;
      --muted: #6b665f;
      --line: #ddd2c2;
      --accent: #0f766e;
      --accent-soft: #d5f2ee;
      --warn: #92400e;
      --warn-soft: #ffedd5;
    }
    body {
      margin: 0;
      background: linear-gradient(180deg, #faf6ee 0%, #efe2cb 100%);
      color: var(--ink);
      font-family: "Pretendard", "Apple SD Gothic Neo", sans-serif;
    }
    .wrap {
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 20px 48px;
    }
    .hero, .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: 0 14px 40px rgba(80, 62, 32, 0.07);
    }
    .hero {
      padding: 28px;
    }
    .hero a {
      color: var(--accent);
      font-weight: 700;
      text-decoration: none;
    }
    h1, h2, h3 {
      margin: 0 0 12px;
    }
    p {
      margin: 0;
      color: var(--muted);
      line-height: 1.7;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 18px;
      margin-top: 22px;
    }
    .wide {
      grid-column: 1 / -1;
    }
    .card {
      padding: 22px;
      display: grid;
      gap: 14px;
    }
    label {
      font-size: 14px;
      font-weight: 700;
      color: var(--ink);
    }
    input, button, select {
      width: 100%;
      box-sizing: border-box;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fff;
      font-size: 14px;
    }
    button {
      border: 0;
      font-weight: 700;
      cursor: pointer;
      background: var(--accent);
      color: #fff;
    }
    .secondary {
      background: #334155;
    }
    .inline {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: end;
    }
    .search-results, .selected-list {
      display: grid;
      gap: 8px;
      margin-top: 6px;
      max-height: 260px;
      overflow: auto;
    }
    .result-item, .selected-item {
      display: grid;
      gap: 6px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }
    .row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
    }
    .mini {
      width: auto;
      min-width: 84px;
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 13px;
    }
    .name {
      font-size: 15px;
      font-weight: 700;
    }
    .meta {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.5;
    }
    .badge {
      display: inline-block;
      width: fit-content;
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
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      font-size: 14px;
      background: #fff;
      border-radius: 18px;
      overflow: hidden;
    }
    th, td {
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      background: #f7f3eb;
      font-size: 13px;
    }
    tr:last-child td {
      border-bottom: 0;
    }
    code, pre {
      font-family: "SFMono-Regular", "Menlo", monospace;
    }
    pre {
      margin: 0;
      padding: 16px;
      border-radius: 18px;
      background: #111827;
      color: #f9fafb;
      overflow: auto;
      min-height: 220px;
      font-size: 13px;
      line-height: 1.55;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
    }
    .summary-box {
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }
    .summary-box strong {
      display: block;
      margin-bottom: 6px;
      font-size: 13px;
      color: var(--muted);
    }
    .hint {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.6;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <span class="badge">별도 서비스 후보</span>
      <h1>KB 단지 상대가치 분석</h1>
      <p>
        기준 단지와 평형을 고른 뒤 비교 단지를 묶어서 현재 <strong>매매가격 비율 차이</strong>와
        <strong>10년 평균 비율</strong>, <strong>상승여력</strong>을 확인합니다.
        단지 기본 정보는 로컬 DB 캐시를 사용하고, 시세 이력은 KB API에서 가져옵니다.
      </p>
      <p style="margin-top: 10px;">
        메인 실행기는 <a href="/">/</a> 에 있고, 이 기능은 <code>/valuation</code> 기준으로 따로 분리할 수 있게 만들었습니다.
      </p>
    </section>

    <section class="grid">
      <div class="card">
        <span class="badge warn">1단계</span>
        <h2>지역 단지 캐시</h2>
        <p>시도/시군구 단위로 단지 기본 정보를 캐시에 저장합니다. 예: <code>경기도 안양시 동안구</code></p>
        <div class="inline">
          <div>
            <label for="sync-region-name">캐시할 지역명</label>
            <input id="sync-region-name" type="text" value="경기도 안양시 동안구">
          </div>
          <button id="sync-region-button" class="secondary" type="button">캐시 갱신</button>
        </div>
        <pre id="sync-output">아직 캐시 동기화 기록이 없습니다.</pre>
      </div>

      <div class="card">
        <span class="badge">2단계</span>
        <h2>기준 단지 선택</h2>
        <label for="base-search-input">기준 단지 검색</label>
        <div class="inline">
          <input id="base-search-input" type="text" placeholder="캐시된 단지명을 검색하세요">
          <button id="base-search-button" type="button">검색</button>
        </div>
        <div id="base-search-results" class="search-results"></div>

        <div class="selected-item">
          <div class="name" id="base-selected-name">기준 단지를 아직 선택하지 않았습니다.</div>
          <div class="meta" id="base-selected-meta">먼저 지역 캐시를 갱신하고 검색 결과에서 기준 단지를 골라주세요.</div>
        </div>

        <label for="base-area-select">기준 평형 / 타입</label>
        <select id="base-area-select" disabled>
          <option value="">기준 단지를 먼저 선택하세요</option>
        </select>
      </div>

      <div class="card wide">
        <span class="badge">3단계</span>
        <h2>비교 단지 선택</h2>
        <div class="inline">
          <input id="compare-search-input" type="text" placeholder="비교 단지명을 검색하세요">
          <button id="compare-search-button" type="button">검색</button>
        </div>
        <div id="compare-search-results" class="search-results"></div>
        <h3>선택된 비교 단지</h3>
        <div id="selected-compare-list" class="selected-list">
          <div class="meta">아직 선택된 비교 단지가 없습니다.</div>
        </div>
      </div>

      <div class="card wide">
        <span class="badge">4단계</span>
        <h2>분석 실행</h2>
        <div class="grid" style="margin-top: 0;">
          <div>
            <label for="analysis-month">기준년월</label>
            <input id="analysis-month" type="text" placeholder="비워두면 최신월 사용, 예: 202603">
          </div>
          <div>
            <label for="lookback-years">평균 계산 기간(년)</label>
            <input id="lookback-years" type="number" min="1" value="10">
          </div>
        </div>
        <div class="row">
          <span class="hint">비교 단지 평형은 기준 단지의 전용면적과 가장 가까운 타입을 자동으로 매칭합니다.</span>
          <button id="analyze-button" type="button" style="width:auto;">상대가치 분석 실행</button>
        </div>
      </div>

      <div class="card wide">
        <h2>분석 요약</h2>
        <div id="analysis-summary" class="summary-grid"></div>
        <div id="analysis-table-wrap"></div>
      </div>

      <div class="card wide">
        <h2>원본 응답</h2>
        <pre id="analysis-json">아직 분석 결과가 없습니다.</pre>
      </div>
    </section>
  </div>

  <script>
    let selectedBase = null;
    let selectedCompareMap = new Map();

    function wonText(value) {
      if (value === null || value === undefined || value === "") return "-";
      return Number(value).toLocaleString("ko-KR");
    }

    function percentText(value) {
      if (value === null || value === undefined || value === "") return "-";
      return `${Number(value).toFixed(2)}%`;
    }

    function regionText(item) {
      return [item.sido_name, item.sigungu_name, item.dong_name].filter(Boolean).join(" ");
    }

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, options);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || payload.detail || JSON.stringify(payload));
      }
      return payload;
    }

    function renderComplexResults(containerId, items, kind) {
      const container = document.getElementById(containerId);
      if (!items.length) {
        container.innerHTML = '<div class="meta">검색 결과가 없습니다.</div>';
        return;
      }

      container.innerHTML = items.map((item) => `
        <div class="result-item">
          <div class="row">
            <div>
              <div class="name">${item.complex_name}</div>
              <div class="meta">${regionText(item)}${item.address ? ` · ${item.address}` : ""}</div>
            </div>
            <button class="mini ${kind === "compare" ? "secondary" : ""}" type="button" data-kind="${kind}" data-complex-id="${item.complex_id}">선택</button>
          </div>
        </div>
      `).join("");
    }

    async function runSearch(kind) {
      const inputId = kind === "base" ? "base-search-input" : "compare-search-input";
      const resultId = kind === "base" ? "base-search-results" : "compare-search-results";
      const query = document.getElementById(inputId).value.trim();
      if (!query) {
        document.getElementById(resultId).innerHTML = '<div class="meta">검색어를 입력하세요.</div>';
        return;
      }
      const payload = await fetchJson(`/valuation/api/search?q=${encodeURIComponent(query)}`);
      renderComplexResults(resultId, payload.items || [], kind);
    }

    async function loadBaseTypes(complexId) {
      const select = document.getElementById("base-area-select");
      select.disabled = true;
      select.innerHTML = '<option value="">불러오는 중...</option>';
      const payload = await fetchJson(`/valuation/api/complex/${complexId}/types`);
      const items = payload.items || [];
      if (!items.length) {
        select.innerHTML = '<option value="">타입 정보를 찾지 못했습니다.</option>';
        return;
      }
      select.innerHTML = items.map((item) => `<option value="${item.area_id}">${item.label}</option>`).join("");
      select.disabled = false;
    }

    function renderSelectedCompareList() {
      const container = document.getElementById("selected-compare-list");
      const items = Array.from(selectedCompareMap.values());
      if (!items.length) {
        container.innerHTML = '<div class="meta">아직 선택된 비교 단지가 없습니다.</div>';
        return;
      }
      container.innerHTML = items.map((item) => `
        <div class="selected-item">
          <div class="row">
            <div>
              <div class="name">${item.complex_name}</div>
              <div class="meta">${regionText(item)}</div>
            </div>
            <button class="mini secondary" type="button" data-remove-complex-id="${item.complex_id}">삭제</button>
          </div>
        </div>
      `).join("");
    }

    function renderSummary(payload) {
      const summary = document.getElementById("analysis-summary");
      const base = payload.base || {};
      summary.innerHTML = `
        <div class="summary-box">
          <strong>기준 단지</strong>
          <div>${base.complex_name || "-"}</div>
        </div>
        <div class="summary-box">
          <strong>기준 평형</strong>
          <div>${base.area || "-"}㎡${base.area_pyeong ? ` / ${base.area_pyeong}평` : ""}</div>
        </div>
        <div class="summary-box">
          <strong>기준월</strong>
          <div>${base.analysis_month_label || "-"}</div>
        </div>
        <div class="summary-box">
          <strong>기준 단지 일반평균가</strong>
          <div>${wonText(base.price)}</div>
        </div>
      `;
    }

    function renderTable(payload) {
      const wrap = document.getElementById("analysis-table-wrap");
      const rows = payload.comparisons || [];
      if (!rows.length) {
        wrap.innerHTML = '<p class="meta">분석 가능한 비교 단지가 없습니다. 비교 단지 선택이나 캐시 상태를 다시 확인해 주세요.</p>';
        return;
      }

      wrap.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>비교 단지</th>
              <th>비교 타입</th>
              <th>비교 매물 가격</th>
              <th>매매가격 비율 차이</th>
              <th>10년 평균 비율</th>
              <th>상승여력</th>
              <th>해석</th>
              <th>기준월</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((item) => `
              <tr>
                <td>
                  <strong>${item.complex_name}</strong><br>
                  <span class="meta">${item.region || "-"}</span>
                </td>
                <td>${item.matched_area || "-"}㎡${item.matched_area_pyeong ? `<br><span class="meta">${item.matched_area_pyeong}평</span>` : ""}</td>
                <td>${wonText(item.compare_price)}</td>
                <td>${percentText(item.current_ratio)}</td>
                <td>${percentText(item.avg_ratio_10y)}</td>
                <td>${percentText(item.upside)}</td>
                <td>${item.label || "-"}</td>
                <td>${item.current_month ? `${item.current_month.slice(0,4)}-${item.current_month.slice(4,6)}` : "-"}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    async function syncRegion() {
      const output = document.getElementById("sync-output");
      const regionName = document.getElementById("sync-region-name").value.trim();
      if (!regionName) {
        output.textContent = "지역명을 입력해 주세요.";
        return;
      }
      output.textContent = "캐시 동기화를 시작했습니다.";
      try {
        const payload = await fetchJson("/valuation/api/sync-region", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ region_name: regionName }),
        });
        output.textContent = JSON.stringify(payload, null, 2);
      } catch (error) {
        output.textContent = JSON.stringify({ success: false, error: String(error) }, null, 2);
      }
    }

    async function analyze() {
      const output = document.getElementById("analysis-json");
      if (!selectedBase) {
        output.textContent = JSON.stringify({ success: false, error: "기준 단지를 먼저 선택해 주세요." }, null, 2);
        return;
      }
      const areaId = document.getElementById("base-area-select").value;
      if (!areaId) {
        output.textContent = JSON.stringify({ success: false, error: "기준 평형을 선택해 주세요." }, null, 2);
        return;
      }
      const compareIds = Array.from(selectedCompareMap.keys());
      if (!compareIds.length) {
        output.textContent = JSON.stringify({ success: false, error: "비교 단지를 1개 이상 선택해 주세요." }, null, 2);
        return;
      }

      const payload = {
        base_complex_id: selectedBase.complex_id,
        base_area_id: Number(areaId),
        compare_complex_ids: compareIds,
        as_of_month: document.getElementById("analysis-month").value.trim() || null,
        lookback_years: Number(document.getElementById("lookback-years").value || 10),
      };

      output.textContent = "분석을 실행하는 중입니다.";
      try {
        const result = await fetchJson("/valuation/api/analyze", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        renderSummary(result);
        renderTable(result);
        output.textContent = JSON.stringify(result, null, 2);
      } catch (error) {
        output.textContent = JSON.stringify({ success: false, error: String(error) }, null, 2);
      }
    }

    document.getElementById("sync-region-button").addEventListener("click", syncRegion);
    document.getElementById("base-search-button").addEventListener("click", () => runSearch("base"));
    document.getElementById("compare-search-button").addEventListener("click", () => runSearch("compare"));
    document.getElementById("analyze-button").addEventListener("click", analyze);

    document.addEventListener("click", async (event) => {
      const target = event.target;
      if (target.dataset.kind === "base") {
        const complexId = Number(target.dataset.complexId);
        const payload = await fetchJson(`/valuation/api/complex/${complexId}`);
        selectedBase = payload.item;
        document.getElementById("base-selected-name").textContent = payload.item.complex_name;
        document.getElementById("base-selected-meta").textContent = `${regionText(payload.item)}${payload.item.address ? ` · ${payload.item.address}` : ""}`;
        await loadBaseTypes(complexId);
      }

      if (target.dataset.kind === "compare") {
        const complexId = Number(target.dataset.complexId);
        const payload = await fetchJson(`/valuation/api/complex/${complexId}`);
        selectedCompareMap.set(complexId, payload.item);
        renderSelectedCompareList();
      }

      if (target.dataset.removeComplexId) {
        selectedCompareMap.delete(Number(target.dataset.removeComplexId));
        renderSelectedCompareList();
      }
    });
  </script>
</body>
</html>
"""


def _parse_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@valuation_bp.route("/", methods=["GET"])
def valuation_index():
    return render_template_string(VALUATION_TEMPLATE)


@valuation_bp.route("/api/search", methods=["GET"])
def valuation_search():
    query = (request.args.get("q") or "").strip()
    limit = _parse_int(request.args.get("limit"), 20) or 20
    items = valuation_service.search_cached_complexes(query, limit=limit)
    return jsonify({"success": True, "items": items})


@valuation_bp.route("/api/complex/<int:complex_id>", methods=["GET"])
def valuation_complex_detail(complex_id: int):
    item = valuation_db.get_complex(complex_id)
    if not item:
        return jsonify({"success": False, "error": "단지 정보를 찾지 못했습니다."}), 404
    return jsonify({"success": True, "item": item})


@valuation_bp.route("/api/complex/<int:complex_id>/types", methods=["GET"])
def valuation_complex_types(complex_id: int):
    force_refresh = str(request.args.get("refresh", "")).lower() in {"1", "true", "y", "yes"}
    items = valuation_service.get_complex_type_options(complex_id, force_refresh=force_refresh)
    return jsonify({"success": True, "items": items})


@valuation_bp.route("/api/sync-region", methods=["POST"])
def valuation_sync_region():
    payload = request.get_json(silent=True) or {}
    region_name = (payload.get("region_name") or "").strip()
    if not region_name:
        return jsonify({"success": False, "error": "region_name 이 필요합니다."}), 400
    sync_types = str(payload.get("sync_types", "")).lower() in {"1", "true", "y", "yes"}
    try:
        result = valuation_service.sync_region_complex_cache(region_name, sync_types=sync_types)
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500
    return jsonify({"success": True, **result})


@valuation_bp.route("/api/analyze", methods=["POST"])
def valuation_analyze():
    payload = request.get_json(silent=True) or {}
    base_complex_id = _parse_int(payload.get("base_complex_id"))
    base_area_id = _parse_int(payload.get("base_area_id"))
    compare_complex_ids = payload.get("compare_complex_ids") or []
    if isinstance(compare_complex_ids, str):
        compare_complex_ids = [value.strip() for value in compare_complex_ids.split(",") if value.strip()]
    compare_complex_ids = [value for value in (_parse_int(item) for item in compare_complex_ids) if value]

    if not base_complex_id or not base_area_id:
        return jsonify({"success": False, "error": "기준 단지와 기준 타입을 먼저 선택해 주세요."}), 400

    try:
        result = valuation_service.analyze_relative_value(
            base_complex_id=base_complex_id,
            base_area_id=base_area_id,
            compare_complex_ids=compare_complex_ids,
            as_of_month=payload.get("as_of_month"),
            lookback_years=_parse_int(payload.get("lookback_years"), 10) or 10,
            force_refresh=str(payload.get("force_refresh", "")).lower() in {"1", "true", "y", "yes"},
        )
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"success": False, "error": str(exc)}), 502
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500

    return jsonify(result)
