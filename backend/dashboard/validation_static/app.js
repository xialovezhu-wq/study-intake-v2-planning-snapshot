"use strict";

const state = { value: null };
const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const detailContent = {
  terra: {
    kicker: "Terra / gate contract",
    title: "规划阶段门禁",
    items: [
      ["前置闭包", "Skill、Capture、release、冲突任务与 formal_write_allowed=false 必须同时通过。"],
      ["工具边界", "Terra 只读冻结任务和编排资产，不直接获得学科数据库 MCP。"],
      ["终态", "planning → waiting_for_luna → integrating → fresh_review → terminal → relocked。"],
    ],
  },
  luna: {
    kicker: "Luna / isolated branches",
    title: "只读分支与波次",
    items: [
      ["独立身份", "每个 branch 独立 child、read_session、STDIO launcher、PID/PGID、transcript 与 receipt。"],
      ["依赖顺序", "同 branch 的 cursor 与 search→get 保持串行；不同 branch 才并发。"],
      ["无丢失", "逻辑分支可超过物理槽位；多余分支排队分波，drop policy 永远为 never。"],
    ],
  },
  sol: {
    kicker: "Sol handoff / export only",
    title: "交接包边界",
    items: [
      ["正常包", "accepted/corrected 导出候选、证据与串行复核提示。"],
      ["风险包", "issues_found 保留 candidate 与 risk；quality_clean=false，不自动重跑。"],
      ["诊断包", "technical_quarantine 只导出诊断，不产生可信业务 handoff。"],
      ["明确禁止", "按钮不调用 Sol 模型，不进入 writer transaction，formal write 始终为 0。"],
    ],
  },
  future: {
    kicker: "Pending live validation",
    title: "未来显式授权后才执行",
    items: [
      ["单任务校准", "创建部署后全新 Capture，运行一条 exact one-shot Terra/Luna task。"],
      ["三科并发", "Math、408、English 各一条全新 Capture、各自独立 authorization。"],
      ["单科多任务", "每个 Task 独立 authorization，验证 fairness、waves 与 no loss。"],
      ["Promotion", "三阶段 receipt 全部可重开后，另行显式批准 production promotion。"],
    ],
  },
};

function stateLabel(value) {
  return ({ passed: "PASS", failed: "FAIL", pending: "PENDING" })[value] || "UNKNOWN";
}

function pill(value) {
  const span = document.createElement("span");
  span.className = "state-pill";
  span.dataset.state = value;
  span.textContent = stateLabel(value);
  return span;
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) node.textContent = String(value ?? "—");
}

function render(value) {
  state.value = value;
  setText("release-value", value.current_release);
  setText("execution-value", value.execution_mode);
  setText("gate-value", value.live_gate);
  setText("authorization-value", value.authorization);
  setText("production-value", String(value.production_accepted).toUpperCase());
  setText("formal-value", value.formal_write_count);
  setText("updated-at", `revision ${value.revision} · 刚刚刷新`);

  for (const subject of ["math", "cs408", "english"]) {
    const row = document.querySelector(`[data-subject-row="${subject}"]`);
    const skill = $("[data-skill-state]", row);
    const mcp = $("[data-mcp-state]", row);
    skill.replaceChildren(pill(value.skills[subject]));
    mcp.replaceChildren(pill(value.mcp_preflight[subject]));
  }

  const stages = { terra: value.terra, luna: value.luna, sol: value.sol_handoff };
  for (const [name, stage] of Object.entries(stages)) {
    const chip = document.querySelector(`[data-stage-state="${name}"]`);
    chip.textContent = stage.state.replaceAll("_", " ");
    const node = document.querySelector(`[data-stage-node="${name}"]`);
    node.dataset.state = stage.state.includes("terminal") || stage.state === "exported" ? "passed" : "locked";
  }
  for (const [name, count] of Object.entries(value.luna.branches)) {
    setText(null, count);
    const node = document.querySelector(`[data-branch="${name}"]`);
    if (node) node.textContent = String(count);
  }
  document.querySelector('[data-action="authorize-terra"]').disabled = !value.terra.action_enabled;
  document.querySelector('[data-action="authorize-luna"]').disabled = !value.luna.action_enabled;
  document.querySelector('[data-action="export-handoff"]').disabled = !value.sol_handoff.action_enabled;
  $("#preflight-button").disabled = value.mode !== "fixture";
  $("#audit-button").disabled = !value.audit_package?.available;

  const remaining = $(".remaining-panel");
  remaining.dataset.complete = String(value.only_real_model_capture_validation_remains);
  if (value.only_real_model_capture_validation_remains) {
    setText("remaining-heading", "只剩真实模型 Capture 验收");
    setText("remaining-copy", "所有非 Live 技术门已通过；真实 Terra/Luna、全新 Capture 和 promotion 仍等待未来显式授权。");
  } else {
    setText("remaining-heading", "等待本轮技术复验");
    setText("remaining-copy", "真实模型 Capture 验收仍被明确锁定；技术状态完成后这里会只保留这一项。");
  }
  $("#error-panel").hidden = true;
}

async function loadState() {
  try {
    const response = await fetch("/api/v1/validation-console/state", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
  } catch (error) {
    $("#error-copy").textContent = `读取失败：${error.message}`;
    $("#error-panel").hidden = false;
    setText("updated-at", "状态不可用");
  }
}

function showDetail(key) {
  const detail = detailContent[key];
  if (!detail) return;
  setText("detail-kicker", detail.kicker);
  setText("detail-title", detail.title);
  const list = document.createElement("ul");
  list.className = "detail-list";
  for (const [label, copy] of detail.items) {
    const item = document.createElement("li");
    const strong = document.createElement("strong");
    strong.textContent = label;
    item.append(strong, document.createTextNode(copy));
    list.append(item);
  }
  $("#detail-body").replaceChildren(list);
  $("#detail-dialog").showModal();
}

function toast(message) {
  const node = $("#toast");
  node.textContent = message;
  node.hidden = false;
  window.setTimeout(() => { node.hidden = true; }, 2800);
}

function nonce() {
  return `ui-${crypto.randomUUID()}`;
}

async function post(path, payload = {}) {
  if (!state.value) throw new Error("状态尚未载入");
  const response = await fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Study-CSRF": state.value.csrf_token,
      "X-Study-Nonce": nonce(),
      "If-Match": String(state.value.revision),
    },
    body: JSON.stringify(payload),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || result.error || `HTTP ${response.status}`);
  return result;
}

$("#refresh-button").addEventListener("click", loadState);
$("#dialog-close").addEventListener("click", () => $("#detail-dialog").close());
$("#detail-dialog").addEventListener("click", (event) => {
  if (event.target === $("#detail-dialog")) $("#detail-dialog").close();
});
$$('[data-detail]').forEach((button) => button.addEventListener("click", () => showDetail(button.dataset.detail)));
$("#future-button").addEventListener("click", () => showDetail("future"));
$("#report-button").addEventListener("click", () => {
  const reports = state.value?.reports || {};
  const first = Object.keys(reports)[0];
  if (first) window.open(`/api/v1/validation-console/reports/${encodeURIComponent(first)}`, "_blank", "noopener");
  else toast("技术报告将在最终部署复验后发布。");
});
$("#audit-button").addEventListener("click", () => {
  if (state.value?.audit_package?.available) {
    window.open("/api/v1/validation-console/audit-package", "_blank", "noopener");
  } else {
    toast("审核包由本轮 Sol 完成重开校验后提供。");
  }
});
$("#preflight-button").addEventListener("click", async () => {
  try { await post("/api/v1/validation-console/preflight"); toast("Fixture 预检完成；未发起真实 MCP 调用。"); await loadState(); }
  catch (error) { toast(error.message); }
});
$("#lock-button").addEventListener("click", async () => {
  try { const result = await post("/api/v1/validation-console/emergency-lock"); toast(`已锁定；撤销 ${result.revoked_stages.length} 个 stage。`); await loadState(); }
  catch (error) { toast(error.message); }
});

loadState();
