const state = {
  app: null,
  form: null,
  preview: null,
  currentRun: null,
  loadedRun: null,
  modelConfigs: {},
  loadouts: [],
  runs: [],
  doctor: null,
  selectedLoadoutSlug: "",
  selectedHistoryRunId: "",
  activePage: "home",
  caseSearch: "",
  caseFilter: "all",
  openCaseIds: new Set(),
  pollTimer: null,
  previewTimer: null,
  serverStopping: false,
};

const ui = {
  tabButtons: Array.from(document.querySelectorAll(".dashboard-tab")),
  tabs: {
    home: document.querySelector("#tab-home"),
    run: document.querySelector("#tab-run"),
    configure: document.querySelector("#tab-configure"),
    loadouts: document.querySelector("#tab-loadouts"),
    doctor: document.querySelector("#tab-doctor"),
  },
  homeTitle: document.querySelector("#home-title"),
  homeSubtitle: document.querySelector("#home-subtitle"),
  homeDescription: document.querySelector("#home-description"),
  homeInfoGrid: document.querySelector("#home-info-grid"),
  homeStatus: document.querySelector("#home-status"),
  homeNote: document.querySelector("#home-note"),
  toolSelect: document.querySelector("#tool-select"),
  homeLoadoutSelect: document.querySelector("#home-loadout-select"),
  datasetInput: document.querySelector("#dataset-input"),
  datasetDefaultButton: document.querySelector("#dataset-default-button"),
  loopInput: document.querySelector("#loop-input"),
  rangeInput: document.querySelector("#range-input"),
  snippetLimitInput: document.querySelector("#snippet-limit-input"),
  pythonCommandInput: document.querySelector("#python-command-input"),
  ragCheckbox: document.querySelector("#rag-checkbox"),
  verboseCheckbox: document.querySelector("#verbose-checkbox"),
  resolvedModel: document.querySelector("#resolved-model"),
  startButton: document.querySelector("#start-button"),
  stopButton: document.querySelector("#stop-button"),
  viewRunButton: document.querySelector("#view-run-button"),
  applyLoadoutButton: document.querySelector("#apply-loadout-button"),
  quitButton: document.querySelector("#quit-button"),

  heroRunTitle: document.querySelector("#hero-run-title"),
  heroRunSubtitle: document.querySelector("#hero-run-subtitle"),
  runNote: document.querySelector("#run-note"),
  runHomeButton: document.querySelector("#run-home-button"),
  runStopButton: document.querySelector("#run-stop-button"),
  runHistorySelect: document.querySelector("#run-history-select"),
  refreshRunsButton: document.querySelector("#refresh-runs-button"),
  loadRunButton: document.querySelector("#load-run-button"),
  resumeRunButton: document.querySelector("#resume-run-button"),
  runInfoGrid: document.querySelector("#run-info-grid"),
  progressLabel: document.querySelector("#progress-label"),
  progressPercent: document.querySelector("#progress-percent"),
  progressFill: document.querySelector("#progress-fill"),
  metricsGrid: document.querySelector("#metrics-grid"),
  perfLine: document.querySelector("#perf-line"),
  researchLine: document.querySelector("#research-line"),
  lastLlmLine: document.querySelector("#last-llm-line"),
  activeCases: document.querySelector("#active-cases"),
  recentActivity: document.querySelector("#recent-activity"),
  caseSearch: document.querySelector("#case-search"),
  caseFilter: document.querySelector("#case-filter"),
  casesScroll: document.querySelector("#cases-scroll"),
  caseRowTemplate: document.querySelector("#case-row-template"),

  configureNote: document.querySelector("#configure-note"),
  configureCards: document.querySelector("#configure-cards"),
  saveModelsButton: document.querySelector("#save-models-button"),
  refreshAllModelsButton: document.querySelector("#refresh-all-models-button"),

  loadoutNameInput: document.querySelector("#loadout-name-input"),
  saveLoadoutButton: document.querySelector("#save-loadout-button"),
  applySelectedLoadoutButton: document.querySelector("#apply-selected-loadout-button"),
  deleteLoadoutButton: document.querySelector("#delete-loadout-button"),
  loadoutList: document.querySelector("#loadout-list"),
  loadoutPreview: document.querySelector("#loadout-preview"),

  doctorSummary: document.querySelector("#doctor-summary"),
  runDoctorButton: document.querySelector("#run-doctor-button"),
  fixDoctorButton: document.querySelector("#fix-doctor-button"),
  doctorBody: document.querySelector("#doctor-body"),
  doctorLog: document.querySelector("#doctor-log"),
};

const PAGE_PATHS = {
  home: "/",
  run: "/run",
  configure: "/configure",
  loadouts: "/loadouts",
  doctor: "/doctor",
};

const dropdowns = {};

function normalizePathname(pathname) {
  if (!pathname || pathname === "/") {
    return "/";
  }
  return pathname.endsWith("/") ? pathname.slice(0, -1) : pathname;
}

function pathToPage(pathname) {
  const normalized = normalizePathname(pathname);
  for (const [pageId, route] of Object.entries(PAGE_PATHS)) {
    if (route !== "/" && normalized === route) {
      return pageId;
    }
  }
  return "home";
}

function syncDocumentTitle() {
  if (state.activePage === "run") {
    const run = displayRun();
    document.title = run?.runId
      ? `FSE AIWare Bench • ${run.runId}`
      : "FSE AIWare Bench • Benchmark View";
    return;
  }
  document.title = "FSE AIWare Bench";
}

function dropdownOptionNodes(dropdown) {
  return Array.from(dropdown.menu.querySelectorAll(".custom-select-option"));
}

function selectedDropdownOption(dropdown) {
  return dropdown.options.find((option) => option.value === dropdown.value) || null;
}

function closeDropdown(dropdown) {
  if (!dropdown) {
    return;
  }
  dropdown.root.classList.remove("is-open");
  dropdown.trigger.setAttribute("aria-expanded", "false");
}

function closeAllDropdowns(except = null) {
  for (const dropdown of Object.values(dropdowns)) {
    if (dropdown && dropdown !== except) {
      closeDropdown(dropdown);
    }
  }
}

function openDropdown(dropdown) {
  if (!dropdown) {
    return;
  }
  closeAllDropdowns(dropdown);
  dropdown.root.classList.add("is-open");
  dropdown.trigger.setAttribute("aria-expanded", "true");
}

function syncDropdownSelection(dropdown) {
  const selected = selectedDropdownOption(dropdown);
  dropdown.label.textContent = selected?.label || dropdown.placeholder;
  for (const node of dropdownOptionNodes(dropdown)) {
    const active = node.dataset.value === dropdown.value;
    node.classList.toggle("is-selected", active);
    node.setAttribute("aria-selected", active ? "true" : "false");
  }
}

function setDropdownValue(dropdown, value, options = {}) {
  const { emit = false } = options;
  let nextValue = value;
  const knownValue = dropdown.options.some((option) => option.value === value);
  if (!knownValue) {
    if (dropdown.allowEmpty) {
      nextValue = "";
    } else {
      nextValue = dropdown.options[0]?.value || "";
    }
  }
  dropdown.value = nextValue;
  syncDropdownSelection(dropdown);
  if (emit && dropdown.onChange) {
    dropdown.onChange(dropdown.value, selectedDropdownOption(dropdown));
  }
}

function renderDropdownOptions(dropdown) {
  if (!dropdown.options.length) {
    dropdown.menu.innerHTML = `<div class="custom-select-empty">No options available.</div>`;
    syncDropdownSelection(dropdown);
    return;
  }
  dropdown.menu.innerHTML = dropdown.options
    .map(
      (option) => `
        <button
          class="custom-select-option${option.value === dropdown.value ? " is-selected" : ""}"
          type="button"
          role="option"
          data-value="${escapeHtml(option.value)}"
          aria-selected="${option.value === dropdown.value ? "true" : "false"}"
        >${escapeHtml(option.label)}</button>
      `,
    )
    .join("");
  syncDropdownSelection(dropdown);
}

function setDropdownOptions(dropdown, options, value = dropdown.value) {
  dropdown.options = options.map((option) => ({
    value: String(option.value ?? ""),
    label: String(option.label ?? option.value ?? ""),
  }));
  setDropdownValue(dropdown, value, { emit: false });
  renderDropdownOptions(dropdown);
}

function focusDropdownOption(dropdown, mode = "selected") {
  const nodes = dropdownOptionNodes(dropdown);
  if (!nodes.length) {
    return;
  }
  let target = null;
  if (mode === "first") {
    target = nodes[0];
  } else if (mode === "last") {
    target = nodes[nodes.length - 1];
  } else {
    target = nodes.find((node) => node.dataset.value === dropdown.value) || nodes[0];
  }
  target.focus();
}

function moveDropdownFocus(dropdown, step) {
  const nodes = dropdownOptionNodes(dropdown);
  if (!nodes.length) {
    return;
  }
  const currentIndex = nodes.indexOf(document.activeElement);
  const baseIndex = currentIndex >= 0 ? currentIndex : 0;
  const nextIndex = (baseIndex + step + nodes.length) % nodes.length;
  nodes[nextIndex].focus();
}

function createDropdown(root, options = {}) {
  const trigger = root.querySelector(".custom-select-trigger");
  const label = root.querySelector(".custom-select-label");
  const menu = root.querySelector(".custom-select-menu");
  const dropdown = {
    root,
    trigger,
    label,
    menu,
    options: [],
    value: "",
    placeholder: root.dataset.placeholder || "Select option",
    allowEmpty: Boolean(options.allowEmpty),
    onChange: options.onChange || null,
  };

  trigger.addEventListener("click", () => {
    if (root.classList.contains("is-open")) {
      closeDropdown(dropdown);
    } else {
      openDropdown(dropdown);
    }
  });

  trigger.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      openDropdown(dropdown);
      focusDropdownOption(dropdown, "first");
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      openDropdown(dropdown);
      focusDropdownOption(dropdown, "last");
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      if (root.classList.contains("is-open")) {
        closeDropdown(dropdown);
      } else {
        openDropdown(dropdown);
        focusDropdownOption(dropdown);
      }
    } else if (event.key === "Escape") {
      closeDropdown(dropdown);
    }
  });

  menu.addEventListener("click", (event) => {
    const option = event.target.closest(".custom-select-option");
    if (!option) {
      return;
    }
    setDropdownValue(dropdown, option.dataset.value || "", { emit: true });
    closeDropdown(dropdown);
    trigger.focus();
  });

  menu.addEventListener("keydown", (event) => {
    if (!event.target.closest(".custom-select-option")) {
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      moveDropdownFocus(dropdown, 1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      moveDropdownFocus(dropdown, -1);
    } else if (event.key === "Home") {
      event.preventDefault();
      focusDropdownOption(dropdown, "first");
    } else if (event.key === "End") {
      event.preventDefault();
      focusDropdownOption(dropdown, "last");
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      const option = event.target.closest(".custom-select-option");
      if (!option) {
        return;
      }
      setDropdownValue(dropdown, option.dataset.value || "", { emit: true });
      closeDropdown(dropdown);
      trigger.focus();
    } else if (event.key === "Escape") {
      event.preventDefault();
      closeDropdown(dropdown);
      trigger.focus();
    }
  });

  syncDropdownSelection(dropdown);
  return dropdown;
}

function setupDropdowns() {
  dropdowns.tool = createDropdown(ui.toolSelect, {
    onChange: (value) => {
      state.form.tool = value;
      requestPreview();
    },
  });
  dropdowns.loadout = createDropdown(ui.homeLoadoutSelect, {
    allowEmpty: true,
    onChange: (value) => {
      state.selectedLoadoutSlug = value;
      renderLoadouts();
    },
  });
  dropdowns.caseFilter = createDropdown(ui.caseFilter, {
    onChange: (value) => {
      state.caseFilter = value || "all";
      renderCases();
    },
  });
  dropdowns.runHistory = createDropdown(ui.runHistorySelect, {
    allowEmpty: true,
    onChange: (value) => {
      state.selectedHistoryRunId = value;
      renderRunPage();
    },
  });
  setDropdownOptions(
    dropdowns.caseFilter,
    [
      { value: "all", label: "All cases" },
      { value: "pass", label: "Passed" },
      { value: "skip", label: "Skipped" },
      { value: "fail", label: "Failed" },
      { value: "outputs", label: "Has outputs" },
    ],
    state.caseFilter,
  );
  setDropdownOptions(dropdowns.runHistory, [], state.selectedHistoryRunId);

  document.addEventListener("click", (event) => {
    const insideDropdown = Object.values(dropdowns).some((dropdown) => dropdown?.root.contains(event.target));
    if (!insideDropdown) {
      closeAllDropdowns();
    }
  });
  window.addEventListener("blur", () => closeAllDropdowns());
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function kvRows(fields) {
  return fields
    .map(
      ([label, value]) => `
        <div class="kv-row">
          <span class="kv-label">${escapeHtml(label)}</span>
          <span class="kv-value">${escapeHtml(value || "-")}</span>
        </div>
      `,
    )
    .join("");
}

function switchPage(pageId, options = {}) {
  const { pushHistory = true, replaceHistory = false } = options;
  state.activePage = pageId;
  for (const button of ui.tabButtons) {
    const active = button.dataset.page === pageId;
    button.classList.toggle("tab-active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  }
  for (const [name, node] of Object.entries(ui.tabs)) {
    const active = name === pageId;
    node.classList.toggle("tab-active", active);
    node.classList.toggle("tab-hidden", !active);
  }
  const path = PAGE_PATHS[pageId] || PAGE_PATHS.home;
  const currentPath = normalizePathname(window.location.pathname);
  if (replaceHistory && currentPath !== path) {
    window.history.replaceState({ pageId }, "", path);
  } else if (pushHistory && currentPath !== path) {
    window.history.pushState({ pageId }, "", path);
  }
  syncDocumentTitle();
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `Request failed: ${response.status} ${response.statusText}`);
  }
  return payload;
}

async function sendJson(url, payload = {}, method = "POST") {
  const response = await fetch(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(body.error || `Request failed: ${response.status} ${response.statusText}`);
  }
  return body;
}

async function deleteJson(url) {
  const response = await fetch(url, { method: "DELETE" });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(body.error || `Request failed: ${response.status} ${response.statusText}`);
  }
  return body;
}

function selectedLoadout() {
  return state.loadouts.find((item) => item.slug === state.selectedLoadoutSlug) || null;
}

function selectedHistoryRun() {
  return state.runs.find((item) => item.runId === state.selectedHistoryRunId) || null;
}

function currentDisplaySource() {
  return state.currentRun && (state.currentRun.runId || state.currentRun.status !== "idle") ? state.currentRun : state.preview;
}

function displayRun() {
  if (isRunActive(state.currentRun)) {
    return state.currentRun;
  }
  return state.loadedRun || state.currentRun || null;
}

function isRunActive(run = state.currentRun) {
  return ["booting", "running", "stopping"].includes(run?.status || "");
}

function populateToolSelect() {
  setDropdownOptions(
    dropdowns.tool,
    (state.app?.tools || []).map((tool) => ({ value: tool, label: tool })),
    state.form?.tool || "",
  );
}

function populateLoadoutSelect() {
  setDropdownOptions(
    dropdowns.loadout,
    state.loadouts.map((loadout) => ({ value: loadout.slug, label: loadout.name })),
    state.selectedLoadoutSlug || "",
  );
}

function populateRunHistorySelect() {
  setDropdownOptions(
    dropdowns.runHistory,
    state.runs.map((run) => ({ value: run.runId, label: run.label })),
    state.selectedHistoryRunId || "",
  );
}

async function refreshRuns() {
  const payload = await fetchJson("/api/runs");
  state.runs = payload.runs || [];
  const stillSelected = state.runs.some((item) => item.runId === state.selectedHistoryRunId);
  if (!stillSelected) {
    state.selectedHistoryRunId = state.runs[0]?.runId || "";
  }
  populateRunHistorySelect();
  renderRunPage();
}

function syncControlsFromForm() {
  if (!state.form) {
    return;
  }
  ui.datasetInput.value = state.form.dataset_tar || "";
  ui.loopInput.value = state.form.loop_count ?? 5;
  ui.rangeInput.value = state.form.search_range ?? 0;
  ui.snippetLimitInput.value = state.form.snippet_limit || "";
  ui.pythonCommandInput.value = state.form.python_command || "";
  ui.ragCheckbox.checked = Boolean(state.form.rag);
  ui.verboseCheckbox.checked = Boolean(state.form.verbose);
  setDropdownValue(dropdowns.tool, state.form.tool || "", { emit: false });
  populateLoadoutSelect();
}

function renderHomeHeader() {
  const run = state.currentRun || {};
  const liveState = run.runId || isRunActive(run) || ["failed", "completed", "stopped"].includes(run.status || "");
  const selectedTool = state.form?.tool || "tool selection";
  ui.homeTitle.textContent = liveState ? run.title || "PyRAG benchmark ready" : "FSE AIWare Command Center";
  ui.homeSubtitle.textContent = liveState
    ? run.subtitle || "Open Benchmark View to inspect progress and results."
    : "Run, report, and configure without memorizing commands.";
  ui.homeDescription.textContent = liveState
    ? run.statusText || "Open the Benchmark View page for live progress, recent activity, and completed cases."
    : `Terminal web dashboard for ${selectedTool}; configure the run, verify Doctor, then start the benchmark.`;
  ui.homeStatus.textContent = run.status || "idle";
  ui.homeInfoGrid.innerHTML = kvRows([
    ["Version", state.app?.versionDisplay || "-"],
    ["Repository", state.app?.repoRoot || "-"],
    ["Tools", (state.app?.tools || []).join(", ") || "-"],
    ["Dataset", state.app?.defaultDatasetLabel || "-"],
    ["Scope", state.app?.server?.scope || "-"],
    ["Local", state.app?.server?.localUrl || "-"],
    ["Network", state.app?.server?.networkUrl || "-"],
  ]);
  ui.homeNote.textContent = run.statusText || "waiting for input";
}

function renderRunHeader() {
  const run = displayRun() || {};
  if (!run.runId) {
    ui.heroRunTitle.textContent = "No run selected";
    ui.heroRunSubtitle.textContent = "Start a benchmark to inspect progress and results.";
  } else {
    ui.heroRunTitle.textContent = run.title || "Benchmark run";
    ui.heroRunSubtitle.textContent = run.subtitle || "Inspect progress and results below.";
  }
  const fields = (currentDisplaySource()?.infoFields || []).map((field) => [field.label, field.value]);
  ui.runInfoGrid.innerHTML = kvRows(fields);
}

function renderProgress() {
  const run = displayRun() || {};
  const source = run.runId ? run : currentDisplaySource() || {};
  const percent = Number(run.progressPercent || 0);
  const config = run.config || state.form || {};

  ui.progressLabel.textContent = `${run.completed ?? 0}/${run.total ?? 0}`;
  ui.progressPercent.textContent = `( ${percent.toFixed(1)}% )`;
  ui.progressFill.style.width = `${Math.min(100, Math.max(0, percent))}%`;
  ui.metricsGrid.innerHTML = `
    <span><span class="kv-label">Successes:</span> <span class="text-green">${escapeHtml(String(run.successes ?? 0))}</span></span>
    <span><span class="kv-label">Failures:</span> <span class="text-red">${escapeHtml(String(run.failures ?? 0))}</span></span>
    <span><span class="kv-label">Skipped:</span> <span class="text-yellow">${escapeHtml(String(run.skipped ?? 0))}</span></span>
    <span><span class="kv-label">Elapsed:</span> <span class="text-yellow">${escapeHtml(run.elapsedLabel || "0m 00s")}</span></span>
    <span><span class="kv-label">Pass rate:</span> <span class="text-yellow">${escapeHtml(run.passRate || "0.0%")}</span></span>
    <span><span class="kv-label">Sec/case:</span> <span class="text-yellow">${escapeHtml(run.speed || "--")}</span></span>
    <span><span class="kv-label">Solve avg:</span> <span class="text-yellow">${escapeHtml(run.solveAverage || "--")}</span></span>
    <span><span class="kv-label">Env avg:</span> <span class="text-yellow">${escapeHtml(run.envCreateAverage || "--")}</span></span>
    <span><span class="kv-label">Install avg:</span> <span class="text-yellow">${escapeHtml(run.installAverage || "--")}</span></span>
    <span><span class="kv-label">Smoke avg:</span> <span class="text-yellow">${escapeHtml(run.smokeAverage || "--")}</span></span>
    <span><span class="kv-label">ETA:</span> <span class="text-yellow">${escapeHtml(run.eta || "--")}</span></span>
  `;
  ui.perfLine.innerHTML = `
    <span class="kv-label">Runtime</span>
    <span class="kv-value">tool=${escapeHtml(config.tool || "-")} loop=${escapeHtml(
      String(config.loop_count ?? "-"),
    )} range=${escapeHtml(String(config.search_range ?? "-"))} rag=${escapeHtml(
      config.rag ? "on" : "off",
    )} verbose=${escapeHtml(config.verbose ? "on" : "off")} solve=${escapeHtml(
      run.solveAverage || "--",
    )} validate=${escapeHtml(run.validationAverage || "--")} env=${escapeHtml(
      run.envCreateAverage || "--",
    )} install=${escapeHtml(run.installAverage || "--")} smoke=${escapeHtml(
      run.smokeAverage || "--",
    )}</span>
  `;
  ui.researchLine.innerHTML = `
    <span class="kv-label">Resolved model</span>
    <span class="kv-value">${escapeHtml(source.resolvedModel || "-")}</span>
  `;
  ui.lastLlmLine.innerHTML = `
    <span class="kv-label">Artifacts</span>
    <span class="kv-value">${escapeHtml(run.runDir || "runs/pending")}</span>
  `;
}

function renderActivityList(container, items, emptyText, formatter) {
  container.innerHTML = "";
  if (!items.length) {
    container.innerHTML = `<div class="empty-line">${escapeHtml(emptyText)}</div>`;
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const item of items) {
    const line = document.createElement("div");
    line.className = "bullet-item";
    line.innerHTML = formatter(item);
    fragment.appendChild(line);
  }
  container.appendChild(fragment);
}

function markerClass(value) {
  if (value === "MATCH" || value === "PASS") {
    return "text-green";
  }
  if (value === "DIFF" || value === "FAIL") {
    return "text-red";
  }
  if (value === "SKIP") {
    return "text-yellow";
  }
  return value && value !== "--" ? "text-yellow" : "text-muted";
}

function statusToneClass(value) {
  if (value === "PASS") {
    return "text-green";
  }
  if (value === "WARN" || value === "SKIP") {
    return "text-yellow";
  }
  return "text-red";
}

function renderCaseDetails(container, item) {
  container.querySelector(".case-critical").innerHTML = kvRows([
    ["Case", item.caseId || "-"],
    ["Snippet", item.snippet || "-"],
    ["Result", item.result || "-"],
    ["Dependencies", item.dependencies || "-"],
    ["Solve", item.solve || "-"],
    ["Validate", item.validation || "-"],
    ["Env create", item.envCreate || "-"],
    ["Install", item.install || "-"],
    ["Smoke", item.smoke || "-"],
    ["PLLM", item.pllmSummary || item.pllm || "-"],
    ["PYEGO", item.legacySummary || item.legacy || "-"],
    ["READPY", item.readpySummary || item.readpy || "-"],
    ["Outputs", String((item.outputFiles || []).length)],
  ]);

  const logTail = item.logTail || [];
  container.querySelector(".attempt-list").innerHTML = `
    <article class="attempt-card">
      <div class="attempt-header">
        <span class="section-title">Execution summary</span>
        <span class="${statusToneClass(item.status)}">${escapeHtml(item.status || "-")}</span>
      </div>
      <div class="attempt-badges">
        <span class="text-yellow">${escapeHtml(item.seconds || "0.0")}s</span>
        <span class="${markerClass(item.pllm)}">${escapeHtml(item.pllm || "-")}</span>
        <span class="${markerClass(item.legacy)}">${escapeHtml(item.legacy || "-")}</span>
        <span class="${markerClass(item.readpy)}">${escapeHtml(item.readpy || "-")}</span>
      </div>
      <div class="attempt-analysis">
        <div class="section-title">Log tail</div>
        <div class="analysis-text">${escapeHtml(logTail.join("\n") || "No log tail captured.")}</div>
      </div>
    </article>
  `;

  const outputs = item.outputFiles || [];
  container.querySelector(".file-list").innerHTML = `
    <div class="section-title">Case artifacts</div>
    <div class="artifact-links">
      ${
        outputs.length
          ? outputs.map((file) => `<span>${escapeHtml(file)}</span>`).join("")
          : '<span>-</span>'
      }
    </div>
  `;
}

function filteredCases() {
  const cases = displayRun()?.completedCases || [];
  return cases.filter((item) => {
    const haystack = [
      item.caseId,
      item.pllm,
      item.pllmSummary,
      item.legacy,
      item.legacySummary,
      item.readpy,
      item.readpySummary,
      item.result,
      item.dependencies,
      item.snippet,
      ...(item.outputFiles || []),
      ...(item.logTail || []),
    ]
      .join(" ")
      .toLowerCase();
    if (state.caseSearch && !haystack.includes(state.caseSearch)) {
      return false;
    }
    switch (state.caseFilter) {
      case "pass":
        return item.status === "PASS";
      case "skip":
        return item.status === "SKIP";
      case "fail":
        return item.status === "FAIL";
      case "outputs":
        return Array.isArray(item.outputFiles) && item.outputFiles.length > 0;
      default:
        return true;
    }
  });
}

function renderCases() {
  const previousScrollTop = ui.casesScroll.scrollTop;
  ui.casesScroll.innerHTML = "";
  const cases = filteredCases();
  if (!cases.length) {
    ui.casesScroll.innerHTML = `<div class="empty-line">No completed cases yet.</div>`;
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const item of cases) {
    const node = ui.caseRowTemplate.content.firstElementChild.cloneNode(true);
    node.dataset.caseId = item.caseId || "";
    const stat = node.querySelector(".case-stat");
    stat.textContent = item.status || "-";
    stat.classList.add(statusToneClass(item.status));
    node.querySelector(".case-id").textContent = item.caseId || "-";
    node.querySelector(".case-python").textContent = item.python || "-";
    node.querySelector(".case-attempts").textContent = item.tries || "-";
    node.querySelector(".case-seconds").textContent = item.seconds || "0.0";

    const pllm = node.querySelector(".case-pllm");
    pllm.textContent = item.pllm || "-";
    pllm.classList.add(markerClass(item.pllm));
    pllm.title = item.pllmSummary || "";

    const pyego = node.querySelector(".case-pyego");
    pyego.textContent = item.legacy || "-";
    pyego.classList.add(markerClass(item.legacy));
    pyego.title = item.legacySummary || "";

    const readpy = node.querySelector(".case-readpy");
    readpy.textContent = item.readpy || "-";
    readpy.classList.add(markerClass(item.readpy));
    readpy.title = item.readpySummary || "";

    node.querySelector(".case-result").textContent = item.result || "-";
    node.querySelector(".case-dependencies").textContent = item.dependencies || "-";

    if (state.openCaseIds.has(item.caseId)) {
      node.open = true;
      renderCaseDetails(node.querySelector(".case-detail"), item);
    }

    node.addEventListener("toggle", () => {
      if (node.open) {
        state.openCaseIds.add(item.caseId);
        renderCaseDetails(node.querySelector(".case-detail"), item);
      } else {
        state.openCaseIds.delete(item.caseId);
      }
    });

    fragment.appendChild(node);
  }
  ui.casesScroll.appendChild(fragment);
  ui.casesScroll.scrollTop = previousScrollTop;
}

function renderHome() {
  renderHomeHeader();
  ui.resolvedModel.textContent = currentDisplaySource()?.resolvedModel || "-";
  ui.startButton.disabled = isRunActive(state.currentRun);
  ui.stopButton.disabled = !isRunActive(state.currentRun);
  ui.viewRunButton.disabled = false;
  syncDocumentTitle();
}

function renderRunPage() {
  const run = displayRun() || {};
  const historyRun = selectedHistoryRun();
  renderRunHeader();
  renderProgress();
  renderActivityList(
    ui.activeCases,
    run.activeCase ? [run.activeCase] : [],
    "No active cases.",
    (item) => `• <span class="kv-value">${escapeHtml(item)}</span>`,
  );
  renderActivityList(
    ui.recentActivity,
    run.recentActivity || [],
    "No recent activity.",
    (item) => `• ${escapeHtml(item)}`,
  );
  renderCases();
  ui.runStopButton.disabled = !isRunActive(state.currentRun);
  ui.refreshRunsButton.disabled = false;
  ui.loadRunButton.disabled = !historyRun || isRunActive(state.currentRun);
  ui.resumeRunButton.disabled = !historyRun || !historyRun.resumable || isRunActive(state.currentRun);
  if (run.runId) {
    ui.runNote.textContent = run.statusText || "Live benchmark output appears on this page.";
  } else if (historyRun) {
    ui.runNote.textContent = `Selected ${historyRun.runId}. Load it to inspect, or resume if cases remain.`;
  } else {
    ui.runNote.textContent = "Live benchmark output appears on this page.";
  }
  syncDocumentTitle();
}

function renderConfigure() {
  ui.configureCards.innerHTML = "";
  const tools = state.app?.tools || [];
  if (!tools.length) {
    ui.configureCards.innerHTML = `<div class="empty-line">No tool model configs available.</div>`;
    return;
  }
  for (const tool of tools) {
    const config = state.modelConfigs[tool] || {};
    const cachedModels = Array.from(new Set([config.model, ...(config.cached_models || [])].filter(Boolean)));
    const datalistId = `models-${tool}`;
    const card = document.createElement("article");
    card.className = "config-card";
    card.dataset.tool = tool;
    card.innerHTML = `
      <div class="warning-title">${escapeHtml(tool)}</div>
      <div class="warning-copy">${
        cachedModels.length ? `${cachedModels.length} cached models ready.` : "Refresh from Ollama to populate model names."
      }</div>
      <div class="toolbar-line">
        <span class="toolbar-label">Base URL</span>
        <input class="long-input" data-field="base_url" type="text" spellcheck="false" value="${escapeHtml(
          config.base_url || "",
        )}" />
        <span class="toolbar-label">Temp</span>
        <input class="compact-input" data-field="temperature" type="number" step="0.1" value="${escapeHtml(
          String(config.temperature ?? 0.7),
        )}" />
        <button class="toolbar-button" data-action="refresh-model" type="button">Refresh</button>
      </div>
      <div class="toolbar-line">
        <span class="toolbar-label">Model</span>
        <input class="long-input" data-field="model" type="text" list="${datalistId}" spellcheck="false" value="${escapeHtml(
          config.model || "",
        )}" />
        <datalist id="${datalistId}">
          ${cachedModels.map((model) => `<option value="${escapeHtml(model)}"></option>`).join("")}
        </datalist>
      </div>
    `;
    ui.configureCards.appendChild(card);
  }
}

function renderLoadouts() {
  populateLoadoutSelect();
  ui.loadoutList.innerHTML = "";
  if (!state.loadouts.length) {
    ui.loadoutList.innerHTML = `<div class="empty-line">No saved loadouts yet.</div>`;
    ui.loadoutPreview.textContent = "No saved loadouts yet.";
    return;
  }
  for (const loadout of state.loadouts) {
    const button = document.createElement("button");
    button.className = `side-button${loadout.slug === state.selectedLoadoutSlug ? " is-selected" : ""}`;
    button.type = "button";
    button.textContent = loadout.name;
    button.addEventListener("click", () => {
      state.selectedLoadoutSlug = loadout.slug;
      setDropdownValue(dropdowns.loadout, loadout.slug, { emit: false });
      renderLoadouts();
    });
    ui.loadoutList.appendChild(button);
  }
  const selected = selectedLoadout();
  if (selected && document.activeElement !== ui.loadoutNameInput) {
    ui.loadoutNameInput.value = selected.name || "";
  }
  ui.loadoutPreview.textContent = selected ? JSON.stringify(selected, null, 2) : "No loadout selected.";
}

function renderDoctor() {
  const doctor = state.doctor || { busy: false, summary: "Doctor has not been run yet.", results: [], logs: [] };
  ui.doctorSummary.textContent = doctor.summary || "Doctor has not been run yet.";
  ui.runDoctorButton.disabled = Boolean(doctor.busy);
  ui.fixDoctorButton.disabled = Boolean(doctor.busy);
  ui.doctorBody.innerHTML = "";
  const rows = doctor.results || [];
  if (!rows.length) {
    ui.doctorBody.innerHTML = `<tr><td colspan="3">No doctor results yet.</td></tr>`;
  } else {
    const fragment = document.createDocumentFragment();
    for (const item of rows) {
      const row = document.createElement("tr");
      const statusClass =
        item.status === "PASS" ? "status-pass" : item.status === "FAIL" ? "status-fail" : "status-warn";
      row.innerHTML = `
        <td class="${statusClass}">${escapeHtml(item.status || "-")}</td>
        <td>${escapeHtml(item.label || "-")}</td>
        <td>${escapeHtml(item.detail || "-")}</td>
      `;
      fragment.appendChild(row);
    }
    ui.doctorBody.appendChild(fragment);
  }
  ui.doctorLog.textContent = (doctor.logs || []).join("\n") || "No automatic setup has been run.";
}

function applyLoadoutToForm(loadout) {
  if (!loadout) {
    return;
  }
  state.form = {
    ...state.form,
    tool: loadout.tool || state.form.tool,
    dataset_tar: loadout.dataset_tar || state.form.dataset_tar,
    loop_count: loadout.loop_count ?? state.form.loop_count,
    search_range: loadout.search_range ?? state.form.search_range,
    rag: Boolean(loadout.rag),
    verbose: Boolean(loadout.verbose),
    snippet_limit: loadout.snippet_limit || "",
    python_command: loadout.python_command || "",
    loadout_name: loadout.name || "",
  };
  state.selectedLoadoutSlug = loadout.slug;
  syncControlsFromForm();
  requestPreview();
  switchPage("home");
}

function currentConfigPayload() {
  return {
    ...state.form,
    loadout_name: state.form?.loadout_name || "",
  };
}

function requestPreview() {
  clearTimeout(state.previewTimer);
  state.previewTimer = window.setTimeout(async () => {
    try {
      state.preview = await sendJson("/api/preview", currentConfigPayload());
      renderHome();
      renderRunPage();
    } catch (error) {
      console.error(error);
    }
  }, 160);
}

async function pollStatus() {
  if (state.serverStopping) {
    return;
  }
  try {
    const previousRunId = state.currentRun?.runId || "";
    const previousStatus = state.currentRun?.status || "";
    const payload = await fetchJson("/api/status");
    state.currentRun = payload.currentRun;
    state.doctor = payload.doctor;
    renderHome();
    renderRunPage();
    renderDoctor();
    if (
      previousRunId &&
      (previousRunId !== (state.currentRun?.runId || "") || previousStatus !== (state.currentRun?.status || "")) &&
      !isRunActive(state.currentRun)
    ) {
      state.selectedHistoryRunId = state.currentRun?.runId || state.selectedHistoryRunId;
      refreshRuns().catch((error) => console.error(error));
    }
  } catch (error) {
    if (!state.serverStopping) {
      console.error(error);
    }
  }
}

function wireTabs() {
  for (const button of ui.tabButtons) {
    button.addEventListener("click", () => switchPage(button.dataset.page || "home"));
  }
  window.addEventListener("popstate", () => {
    switchPage(pathToPage(window.location.pathname), { pushHistory: false });
  });
}

function wireHomeControls() {
  ui.datasetInput.addEventListener("input", () => {
    state.form.dataset_tar = ui.datasetInput.value;
    requestPreview();
  });
  ui.datasetDefaultButton.addEventListener("click", () => {
    state.form.dataset_tar = state.app?.defaultDatasetTar || state.form.dataset_tar;
    syncControlsFromForm();
    requestPreview();
  });
  ui.loopInput.addEventListener("input", () => {
    state.form.loop_count = Number(ui.loopInput.value || 1);
    requestPreview();
  });
  ui.rangeInput.addEventListener("input", () => {
    state.form.search_range = Number(ui.rangeInput.value || 0);
    requestPreview();
  });
  ui.snippetLimitInput.addEventListener("input", () => {
    state.form.snippet_limit = ui.snippetLimitInput.value;
    requestPreview();
  });
  ui.pythonCommandInput.addEventListener("input", () => {
    state.form.python_command = ui.pythonCommandInput.value;
    requestPreview();
  });
  ui.ragCheckbox.addEventListener("change", () => {
    state.form.rag = ui.ragCheckbox.checked;
    requestPreview();
  });
  ui.verboseCheckbox.addEventListener("change", () => {
    state.form.verbose = ui.verboseCheckbox.checked;
    requestPreview();
  });
  ui.applyLoadoutButton.addEventListener("click", () => {
    applyLoadoutToForm(selectedLoadout());
  });
  ui.startButton.addEventListener("click", async () => {
    try {
      const payload = await sendJson("/api/benchmark/start", currentConfigPayload());
      state.currentRun = payload.currentRun;
      state.loadedRun = null;
      state.runs = payload.runs || state.runs;
      state.selectedHistoryRunId = state.currentRun?.runId || state.selectedHistoryRunId;
      populateRunHistorySelect();
      renderHome();
      renderRunPage();
      switchPage("run");
    } catch (error) {
      alert(error.message);
    }
  });
  ui.stopButton.addEventListener("click", async () => {
    try {
      const payload = await sendJson("/api/benchmark/stop");
      state.currentRun = payload.currentRun;
      state.runs = payload.runs || state.runs;
      state.selectedHistoryRunId = state.currentRun?.runId || state.selectedHistoryRunId;
      populateRunHistorySelect();
      renderHome();
      renderRunPage();
    } catch (error) {
      alert(error.message);
    }
  });
  ui.viewRunButton.addEventListener("click", () => {
    switchPage("run");
  });
  ui.quitButton.addEventListener("click", async () => {
    try {
      state.serverStopping = true;
      window.clearInterval(state.pollTimer);
      await sendJson("/api/server/shutdown");
      document.body.innerHTML = `
        <main class="terminal-shell">
          <section class="terminal-frame">
            <div class="terminal-badge">FSE AIWare Benchmark Dashboard</div>
            <section class="terminal-section">
              <div class="terminal-divider"><span>Server Stopped</span></div>
              <div class="warning-title">FSE AIWare Bench</div>
              <div class="warning-copy">The local web server is stopping. You can close this tab.</div>
            </section>
          </section>
        </main>
      `;
    } catch (error) {
      alert(error.message);
    }
  });

  ui.caseSearch.addEventListener("input", (event) => {
    state.caseSearch = event.target.value.trim().toLowerCase();
    renderCases();
  });
  ui.runHomeButton.addEventListener("click", () => {
    switchPage("home");
  });
  ui.runStopButton.addEventListener("click", async () => {
    try {
      const payload = await sendJson("/api/benchmark/stop");
      state.currentRun = payload.currentRun;
      state.runs = payload.runs || state.runs;
      state.selectedHistoryRunId = state.currentRun?.runId || state.selectedHistoryRunId;
      populateRunHistorySelect();
      renderHome();
      renderRunPage();
    } catch (error) {
      alert(error.message);
    }
  });
  ui.refreshRunsButton.addEventListener("click", async () => {
    try {
      await refreshRuns();
    } catch (error) {
      alert(error.message);
    }
  });
  ui.loadRunButton.addEventListener("click", async () => {
    if (!state.selectedHistoryRunId) {
      return;
    }
    try {
      const payload = await fetchJson(`/api/runs/${state.selectedHistoryRunId}`);
      state.loadedRun = payload.run || null;
      state.runs = payload.runs || state.runs;
      state.form = { ...state.form, ...(payload.formConfig || {}) };
      syncControlsFromForm();
      populateRunHistorySelect();
      renderHome();
      renderRunPage();
      switchPage("run");
      requestPreview();
    } catch (error) {
      alert(error.message);
    }
  });
  ui.resumeRunButton.addEventListener("click", async () => {
    if (!state.selectedHistoryRunId) {
      return;
    }
    try {
      const payload = await sendJson(`/api/runs/${state.selectedHistoryRunId}/resume`);
      state.currentRun = payload.currentRun;
      state.loadedRun = null;
      state.runs = payload.runs || state.runs;
      state.selectedHistoryRunId = state.currentRun?.runId || state.selectedHistoryRunId;
      populateRunHistorySelect();
      renderHome();
      renderRunPage();
      switchPage("run");
    } catch (error) {
      alert(error.message);
    }
  });
}

function wireConfigure() {
  ui.configureCards.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-action='refresh-model']");
    if (!button) {
      return;
    }
    const card = button.closest(".config-card");
    const tool = card?.dataset.tool;
    if (!tool) {
      return;
    }
    const baseUrl = card.querySelector("[data-field='base_url']")?.value || "";
    ui.configureNote.textContent = `Refreshing models for ${tool}...`;
    try {
      const payload = await sendJson("/api/models/refresh", { tool, base_url: baseUrl });
      state.modelConfigs = payload.allConfigs || state.modelConfigs;
      ui.configureNote.textContent = payload.models?.length
        ? `Loaded ${payload.models.length} models for ${tool} via ${payload.source}.`
        : payload.error || `No models returned for ${tool}.`;
      renderConfigure();
      requestPreview();
    } catch (error) {
      ui.configureNote.textContent = error.message;
    }
  });

  ui.saveModelsButton.addEventListener("click", async () => {
    const configs = Array.from(ui.configureCards.querySelectorAll(".config-card")).map((card) => ({
      tool: card.dataset.tool,
      base_url: card.querySelector("[data-field='base_url']")?.value || "",
      model: card.querySelector("[data-field='model']")?.value || "",
      temperature: Number(card.querySelector("[data-field='temperature']")?.value || 0.7),
    }));
    try {
      const payload = await sendJson("/api/models/save", { configs });
      state.modelConfigs = payload.modelConfigs || {};
      ui.configureNote.textContent = "Model settings saved under models/.";
      renderConfigure();
      requestPreview();
    } catch (error) {
      ui.configureNote.textContent = error.message;
    }
  });

  ui.refreshAllModelsButton.addEventListener("click", async () => {
    const cards = Array.from(ui.configureCards.querySelectorAll(".config-card"));
    ui.configureNote.textContent = "Refreshing all model lists...";
    for (const card of cards) {
      const tool = card.dataset.tool;
      const baseUrl = card.querySelector("[data-field='base_url']")?.value || "";
      try {
        const payload = await sendJson("/api/models/refresh", { tool, base_url: baseUrl });
        state.modelConfigs = payload.allConfigs || state.modelConfigs;
      } catch (error) {
        ui.configureNote.textContent = `Refresh failed for ${tool}: ${error.message}`;
        renderConfigure();
        return;
      }
    }
    ui.configureNote.textContent = "Refreshed all model lists.";
    renderConfigure();
    requestPreview();
  });
}

function wireLoadouts() {
  ui.saveLoadoutButton.addEventListener("click", async () => {
    try {
      const payload = await sendJson("/api/loadouts/save", {
        name: ui.loadoutNameInput.value,
        config: currentConfigPayload(),
      });
      state.loadouts = payload.loadouts || [];
      state.selectedLoadoutSlug = payload.saved?.slug || "";
      state.form.loadout_name = payload.saved?.name || ui.loadoutNameInput.value || "";
      renderLoadouts();
    } catch (error) {
      alert(error.message);
    }
  });

  ui.applySelectedLoadoutButton.addEventListener("click", () => {
    applyLoadoutToForm(selectedLoadout());
  });

  ui.deleteLoadoutButton.addEventListener("click", async () => {
    const loadout = selectedLoadout();
    if (!loadout) {
      return;
    }
    if (!window.confirm(`Delete loadout "${loadout.name}"?`)) {
      return;
    }
    try {
      const payload = await deleteJson(`/api/loadouts/${loadout.slug}`);
      state.loadouts = payload.loadouts || [];
      state.selectedLoadoutSlug = state.loadouts[0]?.slug || "";
      renderLoadouts();
    } catch (error) {
      alert(error.message);
    }
  });
}

function wireDoctor() {
  ui.runDoctorButton.addEventListener("click", async () => {
    try {
      const payload = await sendJson("/api/doctor/run", {
        tool: state.form.tool,
        python_command: state.form.python_command,
      });
      state.doctor = payload.doctor;
      renderDoctor();
    } catch (error) {
      alert(error.message);
    }
  });

  ui.fixDoctorButton.addEventListener("click", async () => {
    try {
      const payload = await sendJson("/api/doctor/fix", {
        tool: state.form.tool,
        python_command: state.form.python_command,
      });
      state.doctor = payload.doctor;
      renderDoctor();
    } catch (error) {
      alert(error.message);
    }
  });
}

async function initialize() {
  setupDropdowns();
  wireTabs();
  wireHomeControls();
  wireConfigure();
  wireLoadouts();
  wireDoctor();

  const payload = await fetchJson("/api/bootstrap");
  state.app = payload.app;
  state.form = payload.defaultConfig;
  state.preview = payload.homePreview;
  state.currentRun = payload.currentRun;
  state.modelConfigs = payload.modelConfigs || {};
  state.loadouts = payload.loadouts || [];
  state.runs = payload.runs || [];
  state.doctor = payload.doctor;
  state.selectedLoadoutSlug = state.loadouts[0]?.slug || "";
  state.selectedHistoryRunId = state.runs[0]?.runId || "";

  populateToolSelect();
  syncControlsFromForm();
  populateRunHistorySelect();
  renderHome();
  renderRunPage();
  renderConfigure();
  renderLoadouts();
  renderDoctor();
  switchPage(pathToPage(window.location.pathname), { pushHistory: false, replaceHistory: true });

  state.pollTimer = window.setInterval(pollStatus, 1000);
}

initialize().catch((error) => {
  document.body.innerHTML = `
    <main class="terminal-shell">
      <section class="terminal-frame">
        <div class="terminal-badge">FSE AIWare Benchmark Dashboard</div>
        <section class="terminal-section">
          <div class="terminal-divider"><span>Startup Error</span></div>
          <div class="warning-title">Unable to start the web UI</div>
          <div class="warning-copy">${escapeHtml(error.message)}</div>
        </section>
      </section>
    </main>
  `;
});
