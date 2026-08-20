"use strict";

(() => {
  const ITEMS_API_URL = "/api/v1/items";
  const DETAIL_SCHEMA = "study-intake-dashboard-task-detail-v2";
  const LANES = ["waiting", "processing", "completed"];
  const SUBJECTS = ["math", "cs408", "english"];
  const SUBJECT_LABELS = Object.freeze({ math: "数学", cs408: "408", english: "英语" });
  const PHASE_LABELS = Object.freeze({
    idle: "等待处理",
    frozen_evidence: "准备证据",
    ready_for_selection: "等待领取",
    queued: "等待领取",
    dispatching: "准备执行",
    analysis: "Terra 协调与 Luna 读取",
    critical_review: "第二遍审核",
    quality_ready: "报告生成与保存",
    needs_rework: "质量处理",
    failed: "技术故障",
    stale: "执行已终止",
  });
  const DETAIL_SECTIONS = Object.freeze([
    ["summary", "任务摘要"],
    ["timeline", "执行时间线"],
    ["luna_provider", "Luna 与 Provider 调用结果"],
    ["mcp", "MCP 查询证据"],
    ["report", "最终报告"],
    ["quality_findings", "质量 findings"],
    ["sol_review", "Sol review 状态"],
  ]);
  const EVIDENCE_SECTION_TARGETS = Object.freeze({
    capture: "detail-summary",
    luna: "detail-luna_provider",
    mcp: "detail-mcp",
    review: "detail-quality_findings",
    report: "detail-report",
  });

  class DashboardContractError extends Error {
    constructor(message) {
      super(message);
      this.name = "DashboardContractError";
    }
  }

  function hasOwn(value, key) {
    return Object.prototype.hasOwnProperty.call(value, key);
  }

  function requireFields(value, fields, label) {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new DashboardContractError(`${label} 不是对象。`);
    }
    const missing = fields.filter((field) => !hasOwn(value, field));
    if (missing.length) throw new DashboardContractError(`${label} 缺少字段：${missing.join(", ")}`);
  }

  function assertItemsResponse(payload) {
    requireFields(payload, [
      "available", "projection_identity", "study_date", "items",
      "dashboard_request_counter_scope", "dashboard_request_model_call_count",
      "dashboard_request_provider_request_count", "dashboard_request_mcp_tool_call_count",
      "dashboard_request_formal_write_count",
    ], "items API 响应");
    if (payload.available !== true || !Array.isArray(payload.items)) {
      throw new DashboardContractError(`items API 不可用：${payload.error || "unknown"}`);
    }
    if (
      payload.dashboard_request_counter_scope !== "dashboard_read_only_request"
      || payload.dashboard_request_model_call_count !== 0
      || payload.dashboard_request_provider_request_count !== 0
      || payload.dashboard_request_mcp_tool_call_count !== 0
      || payload.dashboard_request_formal_write_count !== 0
    ) {
      throw new DashboardContractError("items API 只读计数合同不成立。");
    }
    requireFields(payload.projection_identity, ["status", "generation", "sha256", "release_id"], "projection_identity");
    if (payload.projection_identity.status !== "verified") {
      throw new DashboardContractError("Dashboard projection 尚未验证。");
    }
  }

  function assertPublicItem(item) {
    requireFields(item, [
      "capture_id", "subject", "queue_state", "current_stage", "local_dispatch_status",
      "execution_status", "quality_status", "report_disposition", "report_available",
      "sol_review_status", "formal_write_eligible", "production_accepted",
    ], `item ${item?.capture_id || "unknown"}`);
    if (!SUBJECTS.includes(item.subject)) {
      throw new DashboardContractError(`item ${item.capture_id} 的 subject 不受支持。`);
    }
    if (item.production_accepted !== false) {
      throw new DashboardContractError(`item ${item.capture_id} 的 production_accepted 不符合只读合同。`);
    }
  }

  function deriveLane(item) {
    assertPublicItem(item);
    const id = item.capture_id;
    if (item.execution_status === "succeeded") {
      if (item.report_available !== true) {
        throw new DashboardContractError(`item ${id} 执行成功但报告不可用。`);
      }
      if (item.quality_status === "passed" && item.report_disposition === "accepted" && item.sol_review_status === "not_required") {
        return "completed";
      }
      if (item.quality_status === "issues_found" && item.report_disposition === "needs_sol_review" && item.sol_review_status === "pending") {
        return "completed";
      }
      throw new DashboardContractError(`item ${id} 的成功质量映射不符合冻结合同。`);
    }
    if (item.execution_status === "failed") {
      if (
        item.quality_status !== "unchecked"
        || item.report_disposition !== "technical_failure"
        || item.sol_review_status !== "not_eligible"
        || typeof item.terminal_error_code !== "string"
        || !item.terminal_error_code
      ) {
        throw new DashboardContractError(`item ${id} 的技术失败映射不完整。`);
      }
      return "technical_failure";
    }
    if (
      item.execution_status !== "running"
      || item.quality_status !== "unchecked"
      || item.report_disposition !== null
      || item.report_available !== false
    ) {
      throw new DashboardContractError(`item ${id} 的运行状态不符合冻结合同。`);
    }
    if (item.queue_state === "queued" && ["pending", "pending_consumer_paused"].includes(item.local_dispatch_status)) {
      return "waiting";
    }
    if (item.queue_state === "running" && ["claimed", "running"].includes(item.local_dispatch_status)) {
      return "processing";
    }
    throw new DashboardContractError(`item ${id} 缺少唯一的等待或处理中映射。`);
  }

  function displayStatus(item, lane) {
    if (lane === "waiting") return "等待领取";
    if (lane === "processing") return "正在处理";
    if (lane === "technical_failure") return "技术失败";
    return item.quality_status === "issues_found" ? "执行成功" : "已完成";
  }

  function qualityLabel(item) {
    if (item.quality_status === "passed") return "质量通过";
    if (item.quality_status === "issues_found") return "等待 Sol 质量复核";
    return "";
  }

  function formatDuration(seconds) {
    if (!Number.isFinite(seconds) || seconds < 0) return "时长未公开";
    if (seconds < 60) return `${Math.floor(seconds)} 秒`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟`;
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return minutes ? `${hours} 小时 ${minutes} 分钟` : `${hours} 小时`;
  }

  function timeLabel(item, lane, nowMs = Date.now()) {
    if (lane === "processing" && Number.isFinite(item.elapsed_runtime_seconds)) {
      return `已运行 ${formatDuration(item.elapsed_runtime_seconds)}`;
    }
    const source = item.started_at || item.captured_at || item.last_state_change_at || item.updated_at;
    const parsed = typeof source === "string" ? Date.parse(source) : Number.NaN;
    if (!Number.isFinite(parsed)) return lane === "waiting" ? "等待时长未公开" : "时间未公开";
    if (lane === "waiting") return `已等待 ${formatDuration(Math.max(0, (nowMs - parsed) / 1000))}`;
    const value = new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date(parsed));
    return lane === "technical_failure" ? `${value} 失败` : `${value} 更新`;
  }

  function evidenceState(status) {
    if (status === "completed") return "complete";
    if (status === "running") return "current";
    if (["failed", "cancelled", "stalled"].includes(status)) return "failed";
    return "unverified";
  }

  function listEvidence(item) {
    const captureState = item.evidence_access_status === "ready"
      ? "complete"
      : ["missing", "quarantined"].includes(item.evidence_access_status) ? "failed" : "unverified";
    return [
      { key: "capture", label: "Capture", state: captureState, summary: `evidence_access_status=${item.evidence_access_status || "unverified"}` },
      { key: "luna", label: "读取分支", state: evidenceState(item.analysis_execution_status), summary: `analysis_execution_status=${item.analysis_execution_status || "not_started"}` },
      { key: "mcp", label: "MCP", state: "unverified", summary: "公共 item 未公开独立 MCP 阶段证据。" },
      { key: "review", label: "审核", state: evidenceState(item.review_execution_status), summary: `review_execution_status=${item.review_execution_status || "not_started"}` },
      { key: "report", label: "报告", state: item.report_available ? "complete" : "unverified", summary: `report_available=${String(item.report_available)}` },
    ];
  }

  function itemToViewModel(item, nowMs = Date.now()) {
    const lane = deriveLane(item);
    return {
      id: item.capture_id,
      subject: item.subject,
      title: item.target_label || item.sentence_id || item.article_id || item.capture_id,
      lane,
      display_status: displayStatus(item, lane),
      phase_label: PHASE_LABELS[item.current_stage] || "阶段未公开",
      time_label: timeLabel(item, lane, nowMs),
      quality_label: qualityLabel(item),
      report_available: item.report_available,
      terminal_error_code: item.terminal_error_code || null,
      error_summary: item.terminal_error_code ? `技术执行失败：${item.terminal_error_code}` : "",
      evidence: listEvidence(item),
      source: item,
    };
  }

  function assertDetailResponse(payload, task) {
    requireFields(payload, [
      "available", "projection_identity", "item",
      "dashboard_request_counter_scope", "dashboard_request_model_call_count",
      "dashboard_request_provider_request_count", "dashboard_request_mcp_tool_call_count",
      "dashboard_request_formal_write_count",
    ], "detail API 响应");
    if (payload.available !== true || !payload.item || payload.item.capture_id !== task.id) {
      throw new DashboardContractError(`item ${task.id} 的详情响应不匹配。`);
    }
    if (
      payload.dashboard_request_counter_scope !== "dashboard_read_only_request"
      || payload.dashboard_request_model_call_count !== 0
      || payload.dashboard_request_provider_request_count !== 0
      || payload.dashboard_request_mcp_tool_call_count !== 0
      || payload.dashboard_request_formal_write_count !== 0
    ) {
      throw new DashboardContractError(`item ${task.id} 的 detail API 只读计数合同不成立。`);
    }
    const detail = payload.item.task_detail;
    if (!detail) return null;
    requireFields(detail, [
      "schema_version", "subject", "capture_id", "identity", "dispatch", "evidence",
      "analysis", "critical_review", "server_queue", "sol_review", "formal_write",
      "progress", "terminal", "artifacts", "generated_at",
    ], `detail ${task.id}`);
    if (detail.schema_version !== DETAIL_SCHEMA || detail.capture_id !== task.id || detail.subject !== task.subject) {
      throw new DashboardContractError(`item ${task.id} 的 detail v2 身份不匹配。`);
    }
    return detail;
  }

  function detailEvidence(detail, item) {
    if (!detail) return listEvidence(item);
    const captureState = detail.evidence.evidence_access_status === "ready"
      ? "complete"
      : ["missing", "quarantined"].includes(detail.evidence.evidence_access_status) ? "failed" : "unverified";
    return [
      { key: "capture", label: "Capture", state: captureState, summary: `evidence_access_status=${detail.evidence.evidence_access_status}` },
      { key: "luna", label: "读取分支", state: evidenceState(detail.analysis.execution_status), summary: `analysis.execution_status=${detail.analysis.execution_status}` },
      { key: "mcp", label: "MCP", state: "unverified", summary: "detail v2 未公开独立 MCP 阶段证据。" },
      { key: "review", label: "审核", state: evidenceState(detail.critical_review.execution_status), summary: `critical_review.execution_status=${detail.critical_review.execution_status}` },
      { key: "report", label: "报告", state: item.report_available ? "complete" : "unverified", summary: `report_available=${String(item.report_available)}` },
    ];
  }

  function shortDigest(value) {
    return typeof value === "string" && value.length >= 12 ? `${value.slice(0, 12)}…` : "未公开";
  }

  function detailSections(task, detail) {
    const item = task.source;
    if (!detail) {
      return {
        summary: [`Capture：${task.id}`, `execution_status=${item.execution_status}`, `quality_status=${item.quality_status}`],
        timeline: ["该任务没有可用的 detail v2。"],
        luna_provider: ["列表仅公开 Analysis 阶段状态；Provider 独立结果未公开。"],
        mcp: ["列表与 detail v2 均未公开独立 MCP 阶段证据。"],
        report: [item.report_available ? "报告已生成；详情暂不可用。" : "报告尚未生成。"],
        quality_findings: [`quality_status=${item.quality_status}`, `report_disposition=${item.report_disposition}`],
        sol_review: [`sol_review_status=${item.sol_review_status}`],
        raw_output: "未请求 raw=1；页面不会自行打开原始结构化输出。",
        technical_evidence: "task detail 不可用。",
      };
    }
    return {
      summary: [`Capture：${detail.capture_id}`, `执行：${item.execution_status}`, `质量：${item.quality_status}`, `报告处置：${item.report_disposition}`],
      timeline: [
        `当前阶段：${detail.dispatch.current_stage}`,
        `本地调度：${detail.dispatch.local_dispatch_status}`,
        `最近有效进展：${detail.progress.last_meaningful_progress_at || "未公开"}`,
        `运行时长：${formatDuration(detail.progress.elapsed_runtime_seconds)}`,
      ],
      luna_provider: [
        `Analysis 执行：${detail.analysis.execution_status}`,
        `Analysis 报告：${detail.analysis.report_status}`,
        `Analysis receipt：${shortDigest(detail.analysis.execution_receipt_sha256)}`,
        "detail v2 未公开独立 Provider 结果；前端不从 Analysis 推断。",
      ],
      mcp: [
        `证据访问：${detail.evidence.evidence_access_status}`,
        `Authority snapshot：${shortDigest(detail.evidence.authority_snapshot_sha256)}`,
        "detail v2 未公开独立 MCP 查询字段；前端不复制 Analysis 证据。",
      ],
      report: [item.report_available ? "报告已生成，可通过本任务详情重新打开证据摘要。" : "报告尚未生成。", `Package：${shortDigest(detail.artifacts.package_sha256)}`],
      quality_findings: [
        `quality_status=${item.quality_status}`,
        `critical_review.execution_status=${detail.critical_review.execution_status}`,
        `critical_review.report_status=${detail.critical_review.report_status}`,
        `critical_review.error_code=${detail.critical_review.error_code || "null"}`,
      ],
      sol_review: [`sol_review_status=${detail.sol_review.status}`, `receipt=${shortDigest(detail.sol_review.receipt_sha256)}`],
      raw_output: "未请求 raw=1；定向实现只读取默认 detail v2，不读取完整结构化输出。",
      technical_evidence: [
        `release_id=${shortDigest(detail.release_id)}`,
        `unit_sha256=${shortDigest(detail.unit_sha256)}`,
        `terminal_receipt=${shortDigest(detail.terminal.receipt_sha256)}`,
      ].join("；"),
    };
  }

  const testExports = { DashboardContractError, assertItemsResponse, deriveLane, itemToViewModel, assertDetailResponse, detailEvidence, detailSections };
  if (typeof module !== "undefined" && module.exports) module.exports = testExports;
  if (typeof document === "undefined") return;

  const state = {
    tasks: [], activeSubject: "all", activeView: "board", activeLane: "waiting",
    lastDialogTrigger: null, projectionIdentity: null, studyDate: null, detailRequestToken: 0,
  };
  const elements = {
    refreshButton: document.querySelector("#refresh-button"),
    refreshStatus: document.querySelector("#refresh-status"),
    subjectButtons: [...document.querySelectorAll("[data-subject]")],
    viewButtons: [...document.querySelectorAll("[data-view]")],
    boardView: document.querySelector("#board-view"),
    errorsView: document.querySelector("#errors-view"),
    errorCount: document.querySelector("#error-count"),
    errorList: document.querySelector("#error-list"),
    laneTabs: [...document.querySelectorAll("[data-lane-tab]")],
    lanes: [...document.querySelectorAll("[data-lane]")],
    dialog: document.querySelector("#task-dialog"),
    dialogClose: document.querySelector("#dialog-close"),
    dialogEyebrow: document.querySelector("#dialog-eyebrow"),
    dialogTitle: document.querySelector("#dialog-title"),
    dialogContent: document.querySelector("#dialog-content"),
    cardTemplate: document.querySelector("#task-card-template"),
  };

  function createElement(tagName, className = "", text = "") {
    const node = document.createElement(tagName);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  }

  function visibleTasks() {
    return state.activeSubject === "all" ? state.tasks : state.tasks.filter((task) => task.subject === state.activeSubject);
  }

  function createEvidenceMini(task) {
    const fragment = document.createDocumentFragment();
    task.evidence.forEach((step) => {
      const marker = createElement("span", "evidence-mini-step");
      marker.dataset.state = step.state;
      fragment.append(marker);
    });
    return fragment;
  }

  function createTaskCard(task) {
    const card = elements.cardTemplate.content.firstElementChild.cloneNode(true);
    card.dataset.taskId = task.id;
    card.setAttribute("aria-label", `${SUBJECT_LABELS[task.subject]}：${task.title}，${task.display_status}，${task.phase_label}`);
    const subject = card.querySelector(".subject-chip");
    subject.textContent = SUBJECT_LABELS[task.subject] || task.subject;
    subject.classList.add(`subject-chip--${task.subject}`);
    card.querySelector(".status-label").textContent = task.display_status;
    card.querySelector(".task-title").textContent = task.title;
    card.querySelector(".phase-label").textContent = task.phase_label;
    card.querySelector(".time-label").textContent = task.time_label;
    card.querySelector(".evidence-mini").replaceChildren(createEvidenceMini(task));
    const report = card.querySelector(".report-label");
    report.textContent = task.report_available ? "报告已生成" : "报告尚未生成";
    report.classList.toggle("is-available", task.report_available);
    const quality = card.querySelector(".quality-label");
    if (task.quality_label) {
      quality.hidden = false;
      quality.textContent = task.quality_label;
      if (task.source.quality_status === "passed") quality.classList.add("is-passed");
    }
    card.addEventListener("click", () => openDialog(task, card));
    return card;
  }

  function renderLane(laneName, tasks) {
    const cards = document.querySelector(`[data-lane-cards="${laneName}"]`);
    document.querySelector(`[data-lane-count="${laneName}"]`).textContent = String(tasks.length);
    document.querySelector(`[data-lane-tab-count="${laneName}"]`).textContent = String(tasks.length);
    cards.replaceChildren();
    if (!tasks.length) {
      cards.append(createElement("p", "empty-state", "当前筛选下没有任务"));
      return;
    }
    const fragment = document.createDocumentFragment();
    tasks.forEach((task) => fragment.append(createTaskCard(task)));
    cards.append(fragment);
  }

  function createErrorCard(task) {
    const card = createElement("button", "error-card");
    card.type = "button";
    card.setAttribute("aria-label", `${SUBJECT_LABELS[task.subject]}：${task.title}，技术失败，${task.error_summary}`);
    const body = createElement("span", "error-card-body");
    const topline = createElement("span", "error-card-topline");
    topline.append(
      createElement("span", `subject-chip subject-chip--${task.subject}`, SUBJECT_LABELS[task.subject] || task.subject),
      createElement("code", "error-code", task.terminal_error_code),
    );
    body.append(topline, createElement("span", "error-card-title", task.title), createElement("p", "", task.error_summary));
    card.append(body, createElement("span", "open-detail-label", "查看证据 →"));
    card.addEventListener("click", () => openDialog(task, card));
    return card;
  }

  function renderErrors(tasks) {
    elements.errorCount.textContent = String(tasks.length);
    elements.errorCount.setAttribute("aria-label", `${tasks.length} 个技术异常`);
    elements.errorList.replaceChildren();
    if (!tasks.length) {
      elements.errorList.append(createElement("p", "empty-state", "当前筛选下没有明确的技术异常"));
      return;
    }
    const fragment = document.createDocumentFragment();
    tasks.forEach((task) => fragment.append(createErrorCard(task)));
    elements.errorList.append(fragment);
  }

  function render() {
    const tasks = visibleTasks();
    LANES.forEach((lane) => renderLane(lane, tasks.filter((task) => task.lane === lane)));
    renderErrors(tasks.filter((task) => task.lane === "technical_failure"));
  }

  function showLoadError(message) {
    LANES.forEach((lane) => document.querySelector(`[data-lane-cards="${lane}"]`).replaceChildren(createElement("p", "load-error", message)));
    elements.errorList.replaceChildren(createElement("p", "load-error", message));
  }

  async function fetchJSON(url) {
    const response = await fetch(url, { cache: "no-store", credentials: "same-origin", headers: { Accept: "application/json" } });
    const payload = await response.json().catch(() => null);
    if (!response.ok) throw new Error(payload?.error || `HTTP ${response.status}`);
    return payload;
  }

  async function loadTasks({ announce = true } = {}) {
    elements.refreshButton.disabled = true;
    elements.refreshButton.setAttribute("aria-busy", "true");
    if (announce) elements.refreshStatus.textContent = "正在读取正式 Dashboard API…";
    try {
      const payload = await fetchJSON(`${ITEMS_API_URL}?subject=all`);
      assertItemsResponse(payload);
      state.tasks = payload.items.map((item) => itemToViewModel(item));
      state.projectionIdentity = payload.projection_identity;
      state.studyDate = payload.study_date;
      render();
      elements.refreshStatus.textContent = `${payload.study_date} · ${payload.items.length} 个任务 · 刚刚刷新`;
    } catch (error) {
      const message = `无法读取正式 Dashboard API：${error instanceof Error ? error.message : "未知错误"}`;
      elements.refreshStatus.textContent = message;
      showLoadError(message);
    } finally {
      elements.refreshButton.disabled = false;
      elements.refreshButton.removeAttribute("aria-busy");
    }
  }

  function detailURL(task) {
    const params = new URLSearchParams({ subject: task.subject, raw: "0" });
    if (state.studyDate) params.set("date", state.studyDate);
    const identity = state.projectionIdentity;
    if (identity?.sha256 && identity?.generation && identity?.release_id) {
      params.set("projection_sha256", identity.sha256);
      params.set("projection_generation", identity.generation);
      params.set("projection_release_id", identity.release_id);
    }
    if (Number.isInteger(task.source.generation) && Number.isInteger(task.source.fence)) {
      params.set("task_generation", String(task.source.generation));
      params.set("task_fence", String(task.source.fence));
    }
    return `${ITEMS_API_URL}/${encodeURIComponent(task.id)}?${params}`;
  }

  function syncUrlState() {
    const url = new URL(window.location.href);
    state.activeSubject === "all" ? url.searchParams.delete("subject") : url.searchParams.set("subject", state.activeSubject);
    state.activeView === "board" ? url.searchParams.delete("view") : url.searchParams.set("view", state.activeView);
    state.activeLane === "waiting" ? url.searchParams.delete("lane") : url.searchParams.set("lane", state.activeLane);
    window.history.replaceState(null, "", url);
  }

  function setActiveSubject(subject, { sync = true } = {}) {
    state.activeSubject = subject;
    elements.subjectButtons.forEach((button) => {
      const selected = button.dataset.subject === subject;
      button.classList.toggle("is-active", selected);
      button.setAttribute("aria-pressed", String(selected));
    });
    render();
    if (sync) syncUrlState();
  }

  function setActiveView(view, { sync = true } = {}) {
    state.activeView = view;
    elements.boardView.hidden = view !== "board";
    elements.errorsView.hidden = view !== "errors";
    elements.viewButtons.forEach((button) => {
      const selected = button.dataset.view === view;
      button.classList.toggle("is-active", selected);
      selected ? button.setAttribute("aria-current", "page") : button.removeAttribute("aria-current");
    });
    if (sync) syncUrlState();
  }

  function setActiveLane(laneName, { moveFocus = false, sync = true } = {}) {
    state.activeLane = laneName;
    elements.lanes.forEach((lane) => lane.classList.toggle("is-active", lane.dataset.lane === laneName));
    elements.laneTabs.forEach((tab) => {
      const selected = tab.dataset.laneTab === laneName;
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
      if (selected && moveFocus) tab.focus();
    });
    if (sync) syncUrlState();
  }

  function applyInitialUrlState() {
    const params = new URLSearchParams(window.location.search);
    if (SUBJECTS.includes(params.get("subject"))) setActiveSubject(params.get("subject"), { sync: false });
    if (["board", "errors"].includes(params.get("view"))) setActiveView(params.get("view"), { sync: false });
    if (LANES.includes(params.get("lane"))) setActiveLane(params.get("lane"), { sync: false });
  }

  function appendDetailSection(container, key, heading, content, options = {}) {
    const section = createElement("section", options.report ? "detail-section detail-section--report" : "detail-section");
    section.id = `detail-${key}`;
    section.append(createElement("h3", "", heading));
    if (Array.isArray(content)) {
      const list = createElement("ul");
      content.forEach((entry) => list.append(createElement("li", "", entry)));
      section.append(list);
    } else {
      section.append(createElement("p", "", content || "正式 API 未提供内容。"));
    }
    container.append(section);
  }

  function createEvidenceRail(task) {
    const rail = createElement("nav", "evidence-rail");
    rail.setAttribute("aria-label", "证据进度线");
    task.evidence.forEach((step) => {
      const button = createElement("button", "evidence-node");
      button.type = "button";
      button.dataset.state = step.state;
      button.setAttribute("aria-label", `${step.label}：${step.summary}`);
      if (step.state === "current") button.setAttribute("aria-current", "step");
      button.append(createElement("span", "evidence-node-dot"), createElement("span", "", step.label));
      button.addEventListener("click", () => document.querySelector(`#${EVIDENCE_SECTION_TARGETS[step.key]}`)?.scrollIntoView({ block: "start", behavior: "smooth" }));
      rail.append(button);
    });
    return rail;
  }

  function renderDialog(task, detail = null, statusMessage = "") {
    const view = { ...task, evidence: detailEvidence(detail, task.source) };
    const sections = detailSections(view, detail);
    elements.dialogEyebrow.textContent = `${SUBJECT_LABELS[task.subject] || task.subject} · 正式 API · 只读`;
    elements.dialogTitle.textContent = task.title;
    elements.dialogContent.replaceChildren();
    const summary = createElement("div", "dialog-summary-row");
    summary.append(
      createElement("span", `subject-chip subject-chip--${task.subject}`, SUBJECT_LABELS[task.subject] || task.subject),
      createElement("span", "detail-status", task.display_status),
      createElement("span", "detail-status", task.time_label),
    );
    if (task.quality_label) summary.append(createElement("span", "detail-status detail-status--quality", task.quality_label));
    elements.dialogContent.append(summary, createEvidenceRail(view));
    if (statusMessage) elements.dialogContent.append(createElement("p", "refresh-status", statusMessage));
    DETAIL_SECTIONS.forEach(([key, heading]) => appendDetailSection(elements.dialogContent, key, heading, sections[key], { report: key === "report" && task.report_available }));
    const raw = createElement("details", "detail-disclosure");
    raw.append(createElement("summary", "", "原始输出"), createElement("p", "", sections.raw_output));
    elements.dialogContent.append(raw);
    const technical = createElement("details", "detail-disclosure");
    technical.append(createElement("summary", "", "技术证据与 receipt"), createElement("p", "", sections.technical_evidence));
    elements.dialogContent.append(technical);
  }

  async function openDialog(task, trigger) {
    state.lastDialogTrigger = trigger;
    const token = ++state.detailRequestToken;
    renderDialog(task, null, task.source.detail_available === false ? "该任务未公开可用详情。" : "正在读取详情…");
    elements.dialog.showModal();
    elements.dialogClose.focus();
    if (task.source.detail_available === false) return;
    try {
      const payload = await fetchJSON(detailURL(task));
      const detail = assertDetailResponse(payload, task);
      if (token === state.detailRequestToken && elements.dialog.open) renderDialog(task, detail, detail ? "" : "正式 API 未返回 task_detail。 ");
    } catch (error) {
      if (token === state.detailRequestToken && elements.dialog.open) {
        renderDialog(task, null, `详情读取失败：${error instanceof Error ? error.message : "未知错误"}`);
      }
    }
  }

  elements.refreshButton.addEventListener("click", () => loadTasks());
  elements.subjectButtons.forEach((button, index) => {
    button.addEventListener("click", () => setActiveSubject(button.dataset.subject));
    button.addEventListener("keydown", (event) => {
      if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
      event.preventDefault();
      const direction = event.key === "ArrowRight" ? 1 : -1;
      const next = elements.subjectButtons[(index + direction + elements.subjectButtons.length) % elements.subjectButtons.length];
      setActiveSubject(next.dataset.subject);
      next.focus();
    });
  });
  elements.viewButtons.forEach((button) => button.addEventListener("click", () => setActiveView(button.dataset.view)));
  elements.laneTabs.forEach((tab, index) => {
    tab.addEventListener("click", () => setActiveLane(tab.dataset.laneTab));
    tab.addEventListener("keydown", (event) => {
      if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      let nextIndex = index;
      if (event.key === "ArrowLeft") nextIndex = (index - 1 + elements.laneTabs.length) % elements.laneTabs.length;
      if (event.key === "ArrowRight") nextIndex = (index + 1) % elements.laneTabs.length;
      if (event.key === "Home") nextIndex = 0;
      if (event.key === "End") nextIndex = elements.laneTabs.length - 1;
      setActiveLane(elements.laneTabs[nextIndex].dataset.laneTab, { moveFocus: true });
    });
  });
  elements.dialogClose.addEventListener("click", () => elements.dialog.close());
  elements.dialog.addEventListener("close", () => {
    state.detailRequestToken += 1;
    if (state.lastDialogTrigger?.isConnected) state.lastDialogTrigger.focus();
    state.lastDialogTrigger = null;
  });

  applyInitialUrlState();
  loadTasks({ announce: false });
})();
