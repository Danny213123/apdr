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
  openLlmCaseIds: new Set(),
  llmFilters: {
    status: "all",
    confidence: "all",
    python: "all",
    search: ""
  },
  deterministicFilters: {
    status: "all",
    tier: "all",
    python: "all",
    search: ""
  },
  pollTimer: null,
  previewTimer: null,
  serverStopping: false,
  sseConnection: null,
  sseReconnectAttempts: 0,
  sseReconnectTimer: null,
  sseConnectionState: "disconnected", // "connecting" | "connected" | "disconnected"
  ssePendingUpdates: [],
  sseUpdateScheduled: false,
  renderCache: {
    deterministicCasesKey: "",
    llmCasesKey: "",
    deterministicCaseIds: [],
    llmCaseIds: [],
  },
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
  validationBackendSelect: document.querySelector("#validation-backend-select"),
  homeLoadoutSelect: document.querySelector("#home-loadout-select"),
  llmPolicyRow: document.querySelector("#llm-policy-row"),
  llmValidationPolicySelect: document.querySelector("#llm-validation-policy-select"),
  llmPolicyNote: document.querySelector("#llm-policy-note"),
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
  llmSummary: document.querySelector("#llm-summary"),
  lastLlmLine: document.querySelector("#last-llm-line"),
  casesCount: document.querySelector("#cases-count"),
  llmCasesCount: document.querySelector("#llm-cases-count"),
  llmCasesScroll: document.querySelector("#llm-cases-scroll"),
  activeCases: document.querySelector("#active-cases"),
  recentActivity: document.querySelector("#recent-activity"),
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

  sseStatusDot: document.querySelector("#sse-status-dot"),
  sseStatusText: document.querySelector("#sse-status-text"),
  sseStatusRegion: document.querySelector("#sse-status-region"),
  streamingBadge: document.querySelector("#streaming-badge"),
  llmStatusFilter: document.querySelector("#llm-status-filter"),
  llmConfidenceFilter: document.querySelector("#llm-confidence-filter"),
  llmPythonFilter: document.querySelector("#llm-python-filter"),
  llmSearchInput: document.querySelector("#llm-search-input"),
  detStatusFilter: document.querySelector("#det-status-filter"),
  detTierFilter: document.querySelector("#det-tier-filter"),
  detPythonFilter: document.querySelector("#det-python-filter"),
  detSearchInput: document.querySelector("#det-search-input"),
  deterministicSuccessValue: document.getElementById("deterministic-success-value"),
  llmSuccessValue: document.getElementById("llm-success-value"),
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
      ? `APDR Bench • ${run.runId}`
      : "APDR Bench • Benchmark View";
    return;
  }
  document.title = "APDR Bench";
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
  const normalized = options.map((option) => ({
    value: String(option.value ?? ""),
    label: String(option.label ?? option.value ?? ""),
  }));
  const sameOptions =
    dropdown.options.length === normalized.length &&
    dropdown.options.every(
      (option, index) =>
        option.value === normalized[index]?.value && option.label === normalized[index]?.label,
    );
  const nextValue = String(value ?? "");
  if (sameOptions && dropdown.value === nextValue) {
    return;
  }
  dropdown.options = normalized;
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

  root.addEventListener("click", (event) => {
    event.stopPropagation();
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
      syncValidationBackendDropdown();
      syncLlmValidationPolicyControl();
      requestPreview();
    },
  });
  dropdowns.validationBackend = createDropdown(ui.validationBackendSelect, {
    onChange: (value) => {
      state.form.validation_backend = value;
      syncLlmValidationPolicyControl();
      requestPreview();
    },
  });
  dropdowns.llmValidationPolicy = createDropdown(ui.llmValidationPolicySelect, {
    onChange: (value) => {
      state.form.llm_validation_policy = value;
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
  dropdowns.runHistory = createDropdown(ui.runHistorySelect, {
    allowEmpty: true,
    onChange: (value) => {
      state.selectedHistoryRunId = value;
      renderRunPage();
    },
  });
  setDropdownOptions(dropdowns.runHistory, [], state.selectedHistoryRunId);
  syncValidationBackendDropdown();
  syncLlmValidationPolicyControl();

  document.addEventListener("click", (event) => {
    const insideDropdown = Object.values(dropdowns).some((dropdown) => dropdown?.root.contains(event.target));
    if (!insideDropdown) {
      closeAllDropdowns();
    }
  });
  window.addEventListener("blur", () => closeAllDropdowns());
}

function setupCustomSelect(root, onChange) {
  const dropdown = createDropdown(root, { onChange });
  const options = Array.from(root.querySelectorAll(".custom-select-option")).map(option => ({
    value: option.dataset.value || "",
    label: option.textContent || ""
  }));
  setDropdownOptions(dropdown, options, "all");
  return dropdown;
}

function setupLLMFilters() {
  // Status filter
  setupCustomSelect(ui.llmStatusFilter, (value) => {
    state.llmFilters.status = value;
    applyLLMFilters({ force: true });
  });

  // Confidence filter
  setupCustomSelect(ui.llmConfidenceFilter, (value) => {
    state.llmFilters.confidence = value;
    applyLLMFilters({ force: true });
  });

  // Python filter
  setupCustomSelect(ui.llmPythonFilter, (value) => {
    state.llmFilters.python = value;
    applyLLMFilters({ force: true });
  });

  // Search input (debounced 150ms)
  let searchDebounce = null;
  ui.llmSearchInput.addEventListener("input", (e) => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
      state.llmFilters.search = e.target.value.toLowerCase();
      applyLLMFilters({ force: true });
    }, 150);
  });
}

function applyLLMFilters(options = {}) {
  const { force = false } = options;
  const run = displayRun();
  if (!run || !run.completedCases) {
    renderLlmCases([], { force });
    return;
  }

  const filtered = run.completedCases.filter(caseData => {
    // Filter by tier (LLM = tier3 only)
    const tier = caseData.tier || "unknown";
    if (tier !== "tier3") return false;

    // Status filter
    if (state.llmFilters.status !== "all") {
      if (caseData.status?.toUpperCase() !== state.llmFilters.status.toUpperCase()) return false;
    }

    // Confidence filter
    if (state.llmFilters.confidence !== "all") {
      const conf = caseData.confidence;
      if (state.llmFilters.confidence === "high" && (conf === null || conf <= 0.7)) return false;
      if (state.llmFilters.confidence === "medium" && (conf === null || conf < 0.4 || conf > 0.7)) return false;
      if (state.llmFilters.confidence === "low" && (conf === null || conf >= 0.4)) return false;
      if (state.llmFilters.confidence === "skipped" && caseData.status?.toUpperCase() !== "SKIP") return false;
    }

    // Python filter
    if (state.llmFilters.python !== "all") {
      const pyVersion = caseData.python || "";
      if (pyVersion !== state.llmFilters.python) return false;
    }

    // Search filter
    if (state.llmFilters.search) {
      const searchTerm = state.llmFilters.search;
      const caseId = (caseData.caseId || "").toLowerCase();
      const confidence = (caseData.confidence || "").toString();
      const result = (caseData.result || "").toLowerCase();
      const deps = (caseData.dependencies || "").toLowerCase();

      if (!caseId.includes(searchTerm) &&
          !confidence.includes(searchTerm) &&
          !result.includes(searchTerm) &&
          !deps.includes(searchTerm)) {
        return false;
      }
    }

    return true;
  });

  renderLlmCases(filtered, { force });
}

function setupDeterministicFilters() {
  // Status filter
  setupCustomSelect(ui.detStatusFilter, (value) => {
    state.deterministicFilters.status = value;
    applyDeterministicFilters({ force: true });
  });

  // Tier filter
  setupCustomSelect(ui.detTierFilter, (value) => {
    state.deterministicFilters.tier = value;
    applyDeterministicFilters({ force: true });
  });

  // Python filter
  setupCustomSelect(ui.detPythonFilter, (value) => {
    state.deterministicFilters.python = value;
    applyDeterministicFilters({ force: true });
  });

  // Search input (debounced 150ms per UI-SPEC performance contract)
  let searchDebounce = null;
  ui.detSearchInput.addEventListener("input", (e) => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
      state.deterministicFilters.search = e.target.value.toLowerCase();
      applyDeterministicFilters({ force: true });
    }, 150);
  });
}

function applyDeterministicFilters(options = {}) {
  const { force = false } = options;
  const run = displayRun();
  if (!run || !run.completedCases) {
    renderDeterministicCases([], { force });
    return;
  }

  const filtered = run.completedCases.filter(caseData => {
    // Filter by tier (deterministic = tier1 or tier2)
    const tier = caseData.tier || "unknown";
    if (tier !== "tier1" && tier !== "tier2") return false;

    // Status filter
    if (state.deterministicFilters.status !== "all") {
      if (caseData.status?.toUpperCase() !== state.deterministicFilters.status.toUpperCase()) return false;
    }

    // Tier filter
    if (state.deterministicFilters.tier !== "all") {
      if (tier !== state.deterministicFilters.tier) return false;
    }

    // Python filter
    if (state.deterministicFilters.python !== "all") {
      const pyVersion = caseData.python || "";
      if (pyVersion !== state.deterministicFilters.python) return false;
    }

    // Search filter (case ID, snippet content, dependencies)
    if (state.deterministicFilters.search) {
      const searchTerm = state.deterministicFilters.search;
      const caseId = (caseData.caseId || "").toLowerCase();
      const result = (caseData.result || "").toLowerCase();
      const deps = (caseData.dependencies || "").toLowerCase();

      if (!caseId.includes(searchTerm) &&
          !result.includes(searchTerm) &&
          !deps.includes(searchTerm)) {
        return false;
      }
    }

    return true;
  });

  renderDeterministicCases(filtered, { force });
}

function renderConfidenceBadge(confidence, skipReason) {
  if (skipReason) {
    return `<span class="skip-badge" data-tooltip="Skipped: ${escapeHtml(skipReason)}">⊘</span>`;
  }

  if (confidence === null || confidence === undefined) {
    return `<span class="confidence-badge" data-tooltip="No confidence data"></span>`;
  }

  let level = "low";
  let tooltip = `Low confidence: ${confidence.toFixed(2)}`;
  if (confidence > 0.7) {
    level = "high";
    tooltip = `High confidence: ${confidence.toFixed(2)}`;
  } else if (confidence >= 0.4) {
    level = "medium";
    tooltip = `Medium confidence: ${confidence.toFixed(2)}`;
  }

  return `<span class="confidence-badge ${level}" data-tooltip="${tooltip}"></span>`;
}

function renderCacheBadge(cached) {
  if (!cached) return "";
  return `<span class="cache-badge" data-tooltip="Cached: import combination previously resolved">⚡</span>`;
}

function renderCaseDetail(caseData) {
  let html = '<div class="case-detail">';

  // Resolution path
  if (caseData.resolutionPath) {
    html += '<div class="resolution-path-view">';
    html += '<div class="section-title">Resolution path</div>';
    caseData.resolutionPath.forEach(step => {
      const cssClass = step.result === "match" ? "success" : "attempted";
      html += `<div class="resolution-path-item ${cssClass}">${escapeHtml(step.tier)} → ${escapeHtml(step.result)}</div>`;
    });
    html += '</div>';
  }

  // Confidence breakdown
  if (caseData.confidenceBreakdown) {
    html += '<div class="confidence-breakdown">';
    html += '<div class="section-title" style="grid-column: 1 / -1;">Confidence scoring</div>';
    Object.entries(caseData.confidenceBreakdown).forEach(([key, value]) => {
      html += `<div class="label">${escapeHtml(key)}:</div><div class="value">${value.toFixed(2)}</div>`;
    });
    html += '</div>';
  }

  // LLM prompt/response
  if (caseData.llmPrompt) {
    html += '<div class="section-title">LLM prompt</div>';
    html += `<pre class="llm-prompt-response">${escapeHtml(caseData.llmPrompt)}</pre>`;
  }
  if (caseData.llmResponse) {
    html += '<div class="section-title">LLM response</div>';
    html += `<pre class="llm-prompt-response">${escapeHtml(caseData.llmResponse)}</pre>`;
  }

  html += '</div>';
  return html;
}

function setupCaseRowExpansion(rowElement, caseData) {
  rowElement.addEventListener("click", () => {
    rowElement.classList.toggle("expanded");
    if (rowElement.classList.contains("expanded") && !rowElement.querySelector(".case-detail")) {
      rowElement.insertAdjacentHTML("beforeend", renderCaseDetail(caseData));
    }
  });
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

function validationPathParts(item) {
  return String(item?.validationPath || "")
    .split("->")
    .map((part) => part.trim())
    .filter(Boolean);
}

function validationRouteSummary(item) {
  const path = String(item?.validationPath || "").trim();
  if (path) {
    return path;
  }

  const backend = String(item?.validationBackend || "").trim();
  if (backend && backend !== "llm") {
    return backend;
  }

  const route = String(item?.llmValidationRoute || "").trim();
  const requestedPolicy = String(item?.requestedLlmValidationPolicy || "").trim();
  const dockerStatus = String(item?.dockerStatus || "").trim();

  if (route.startsWith("env-first") || requestedPolicy === "env-first") {
    return dockerStatus === "attempted" ? "env->docker" : "env";
  }
  if (route === "docker-first" || requestedPolicy === "docker-first") {
    return "docker";
  }

  return backend;
}

function validationFirstHop(item) {
  const parts = validationPathParts(item);
  if (parts.length) {
    return parts[0];
  }

  const route = String(item?.llmValidationRoute || "").trim();
  const requestedPolicy = String(item?.requestedLlmValidationPolicy || "").trim();
  if (route.startsWith("env-first") || requestedPolicy === "env-first") {
    return "env";
  }
  if (route === "docker-first" || requestedPolicy === "docker-first") {
    return "docker";
  }

  return validationRouteSummary(item);
}

function validationTerminalHop(item) {
  const parts = validationPathParts(item);
  if (parts.length) {
    return parts[parts.length - 1];
  }
  return validationRouteSummary(item);
}

function validationEnvStatus(item) {
  const envBuilds = Number(item?.envBuilds || 0);
  if (envBuilds > 0) {
    return envBuilds === 1 ? "attempted (1 build)" : `attempted (${envBuilds} builds)`;
  }

  const route = String(item?.llmValidationRoute || "").trim();
  const backend = String(item?.validationBackend || "").trim();
  if (validationPathParts(item).includes("env") || route.startsWith("env-first") || backend === "env") {
    return "selected";
  }
  if (backend === "tier1-cache" || backend === "import-set-cache") {
    return "not needed";
  }
  return "";
}

function validationTruthFields(item) {
  const hasLlmTruth =
    item.validationBackend === "llm" ||
    item.dockerPlanStatus ||
    item.dockerPlanPath ||
    item.requestedLlmValidationPolicy ||
    item.llmValidationRoute ||
    item.dockerStatus ||
    item.dockerBypassReason ||
    item.dockerBypassNote ||
    item.authoredDockerfilePath ||
    item.executedDockerfilePath ||
    item.executedImageRef;
  if (!hasLlmTruth) {
    return [];
  }

  const fields = [
    ["Validation backend", item.validationBackend],
    ["Route summary", validationRouteSummary(item)],
    ["First hop", validationFirstHop(item)],
    ["Final hop", validationTerminalHop(item)],
    ["Env status", validationEnvStatus(item)],
    ["Docker plan", item.dockerPlanStatus],
    ["Docker plan path", item.dockerPlanPath],
    ["Docker plan authorship", item.dockerPlanAuthorship],
    ["Docker plan fallback", Array.isArray(item.dockerPlanFallbackSections) ? item.dockerPlanFallbackSections.join(", ") : item.dockerPlanFallbackSections],
    ["Authored Dockerfile", item.authoredDockerfilePath],
    ["Executed Dockerfile", item.executedDockerfilePath],
    ["Build command", item.dockerBuildCommandPath],
    ["Run command", item.dockerRunCommandPath],
    ["Executed image", item.executedImageRef],
    ["Image handoff", item.imageHandoffVerified === true ? "verified" : item.imageHandoffVerified === false && item.executedImageRef ? "not verified" : ""],
    ["Image inspect", item.imageInspectPath],
    ["Requested policy", item.requestedLlmValidationPolicy],
    ["Validation path", item.validationPath],
    ["LLM route", item.llmValidationRoute],
    ["Docker status", item.dockerStatus],
    ["Docker bypass", item.dockerBypassReason],
    ["Failure family", item.failureFamily],
    ["Result origin", item.resultOrigin],
    ["Debug dir", item.debugDir],
    ["Docker bypass note", item.dockerBypassNote],
  ];
  return fields.filter(([, value]) => value);
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
  if (pageId === "home") {
    renderHome();
  } else if (pageId === "run") {
    renderRunPage();
  } else if (pageId === "configure") {
    renderConfigure();
  } else if (pageId === "loadouts") {
    renderLoadouts();
  } else if (pageId === "doctor") {
    renderDoctor();
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

function llmCasesForRun(run = displayRun()) {
  return Array.isArray(run?.llmCases) ? run.llmCases : [];
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

function validationBackendOptions(tool) {
  if (tool === "pllm") {
    return [{ value: "docker", label: "Docker" }];
  }
  if (tool === "apdr") {
    return [
      { value: "env", label: "Local envs" },
      { value: "docker", label: "Docker" },
      { value: "llm", label: "LLM resolver" },
      { value: "llm-only", label: "LLM-only" },
    ];
  }
  return [{ value: "env", label: "Local envs" }];
}

function syncValidationBackendDropdown() {
  if (!dropdowns.validationBackend) {
    return;
  }
  const tool = state.form?.tool || "";
  const options = validationBackendOptions(tool);
  const requested = state.form?.validation_backend || options[0]?.value || "env";
  const allowed = options.some((option) => option.value === requested);
  const nextValue = allowed ? requested : (options[0]?.value || "env");
  if (state.form) {
    state.form.validation_backend = nextValue;
  }
  setDropdownOptions(dropdowns.validationBackend, options, nextValue);
}

function showLlmValidationPolicyControl() {
  return false;
}

function llmValidationPolicyOptions() {
  return [
    { value: "env-first", label: "env-first" },
  ];
}

function syncLlmValidationPolicyControl() {
  if (!dropdowns.llmValidationPolicy || !ui.llmPolicyRow || !state.form) {
    return;
  }
  const nextValue = "env-first";
  state.form.llm_validation_policy = nextValue;
  setDropdownOptions(dropdowns.llmValidationPolicy, llmValidationPolicyOptions(), nextValue);
  ui.llmPolicyRow.hidden = !showLlmValidationPolicyControl();
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
  syncValidationBackendDropdown();
  syncLlmValidationPolicyControl();
  populateLoadoutSelect();
}

function renderHomeHeader() {
  const run = state.currentRun || {};
  const liveState = run.runId || isRunActive(run) || ["failed", "completed", "stopped"].includes(run.status || "");
  const selectedTool = state.form?.tool || "tool selection";
  ui.homeTitle.textContent = liveState ? run.title || "APDR benchmark ready" : "APDR Command Center";
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
    <span><span class="kv-label">LLM calls:</span> <span class="text-yellow">${escapeHtml(String(run.totalLlmCalls ?? 0))}</span></span>
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
    )} range=${escapeHtml(String(config.search_range ?? "-"))} backend=${escapeHtml(
      config.llm_only_mode ? "llm-only" : (config.validation_backend || "-"),
    )} rag=${escapeHtml(
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

function renderLlmInsights(run = displayRun() || {}) {
  const llmCases = llmCasesForRun(run);
  const totalCalls = Number(run.totalLlmCalls || 0);
  const totalRetries = Number(run.totalRetries || 0);
  const retryCases = Number(run.casesWithLlmRetries || 0);

  if (!llmCases.length) {
    ui.llmSummary.textContent = totalCalls > 0 ? `${totalCalls} total LLM calls recorded.` : "No completed LLM calls yet.";
    return;
  }

  ui.llmSummary.textContent =
    `${totalCalls} total call${totalCalls === 1 ? "" : "s"} across ${llmCases.length} ` +
    `case${llmCases.length === 1 ? "" : "s"}; ${totalRetries} retr${totalRetries === 1 ? "y" : "ies"} ` +
    `across ${retryCases} case${retryCases === 1 ? "" : "s"}.`;
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
    ["Validation backend", item.validationBackend || "-"],
    ["Validation route", validationRouteSummary(item) || "-"],
    ["First hop", validationFirstHop(item) || "-"],
    ["Final hop", validationTerminalHop(item) || "-"],
    ["Env status", validationEnvStatus(item) || "-"],
    ["Docker status", item.dockerStatus || "-"],
    ["LLM calls", item.llmCalls || "0"],
    ["Retries", item.retries || "0"],
    ["Env builds", item.envBuilds || "0"],
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

  const truthFields = validationTruthFields(item);
  const logTail = item.logTail || [];
  container.querySelector(".attempt-list").innerHTML = `
    ${
      truthFields.length
        ? `
      <article class="attempt-card">
        <div class="attempt-header">
          <span class="section-title">Validation truth</span>
        </div>
        <div class="attempt-analysis">
          ${kvRows(truthFields)}
        </div>
      </article>
    `
        : ""
    }
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
      item.llmCalls,
      item.retries,
      item.envBuilds,
      item.validationBackend,
      item.validationPath,
      item.dockerStatus,
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

function buildCaseRow(item, openIds) {
  const node = ui.caseRowTemplate.content.firstElementChild.cloneNode(true);
  node.dataset.caseId = item.caseId || "";
  const summaryButton = node.querySelector(".case-summary");
  const stat = node.querySelector(".case-stat");
  stat.textContent = item.status || "-";
  stat.classList.add(statusToneClass(item.status));

  // Add tier badge
  const tier = item.tier || "unknown";
  const tierSpan = node.querySelector(".case-tier");
  if (tier === "tier1" || tier === "tier2" || tier === "tier3") {
    tierSpan.innerHTML = `<span class="tier-badge ${tier}">${tier.toUpperCase()}</span>`;
  } else {
    tierSpan.textContent = "-";
  }

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

  const route = validationRouteSummary(item) || "-";
  const routeNode = node.querySelector(".case-route");
  routeNode.textContent = route;
  routeNode.title = route;

  node.querySelector(".case-result").textContent = item.result || "-";
  node.querySelector(".case-dependencies").textContent = item.dependencies || "-";

  if (openIds.has(item.caseId)) {
    node.classList.add("expanded");
    summaryButton.setAttribute("aria-expanded", "true");
    renderCaseDetails(node.querySelector(".case-detail"), item);
  }

  summaryButton.addEventListener("click", () => {
    const isOpen = node.classList.toggle("expanded");
    summaryButton.setAttribute("aria-expanded", isOpen ? "true" : "false");
    if (isOpen) {
      openIds.add(item.caseId);
      renderCaseDetails(node.querySelector(".case-detail"), item);
    } else {
      openIds.delete(item.caseId);
    }
  });

  return node;
}

function renderCases() {
  // Redirect to new deterministic filters (Phase 2)
  applyDeterministicFilters();
}

function caseRenderKey(items, fields) {
  return items
    .map((item) =>
      fields
        .map((field) => {
          const value = item[field];
          return value === undefined || value === null ? "" : String(value);
        })
        .join("~"),
    )
    .join("|");
}

function canAppendCases(previousIds, nextIds) {
  if (!previousIds.length || nextIds.length < previousIds.length) {
    return false;
  }
  for (let index = 0; index < previousIds.length; index += 1) {
    if (previousIds[index] !== nextIds[index]) {
      return false;
    }
  }
  return nextIds.length > previousIds.length;
}

function renderDeterministicCases(filtered, options = {}) {
  const { force = false } = options;
  const nextIds = filtered.map((item) => item.caseId || "");
  const nextKey = caseRenderKey(filtered, [
    "caseId",
    "status",
    "tier",
    "python",
    "tries",
    "seconds",
    "validationBackend",
    "validationPath",
    "result",
    "dependencies",
    "pllm",
    "legacy",
    "readpy",
  ]);
  ui.casesCount.textContent = `(${filtered.length})`;
  if (!force && state.renderCache.deterministicCasesKey === nextKey) {
    return;
  }
  if (
    !force &&
    canAppendCases(state.renderCache.deterministicCaseIds, nextIds) &&
    ui.casesScroll.children.length > 0
  ) {
    const previousScrollTop = ui.casesScroll.scrollTop;
    const fragment = document.createDocumentFragment();
    for (const item of filtered.slice(state.renderCache.deterministicCaseIds.length)) {
      fragment.appendChild(buildCaseRow(item, state.openCaseIds));
    }
    ui.casesScroll.appendChild(fragment);
    ui.casesScroll.scrollTop = previousScrollTop;
    state.renderCache.deterministicCasesKey = nextKey;
    state.renderCache.deterministicCaseIds = nextIds;
    return;
  }
  state.renderCache.deterministicCasesKey = nextKey;
  state.renderCache.deterministicCaseIds = nextIds;
  const previousScrollTop = ui.casesScroll.scrollTop;
  ui.casesScroll.innerHTML = "";
  if (!filtered.length) {
    state.renderCache.deterministicCaseIds = [];
    ui.casesScroll.innerHTML = `<div class="empty-line">No deterministic cases match current filters.</div>`;
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const item of filtered) {
    fragment.appendChild(buildCaseRow(item, state.openCaseIds));
  }
  ui.casesScroll.appendChild(fragment);
  ui.casesScroll.scrollTop = previousScrollTop;
}

function buildLLMCaseRow(item, openIds) {
  const node = ui.caseRowTemplate.content.firstElementChild.cloneNode(true);
  node.dataset.caseId = item.caseId || "";
  const summaryButton = node.querySelector(".case-summary");
  const stat = node.querySelector(".case-stat");

  // Add confidence badge + cache badge to status column
  const confBadgeHTML = renderConfidenceBadge(item.confidence, item.skipReason);
  const cacheBadgeHTML = renderCacheBadge(item.cached);
  stat.innerHTML = `<span class="${statusToneClass(item.status)}">${escapeHtml(item.status || "-")}</span>${cacheBadgeHTML}`;

  // Add confidence badge to CONF column (uses .case-tier selector which maps to CONF column in LLM table)
  const confSpan = node.querySelector(".case-tier");
  confSpan.innerHTML = confBadgeHTML;

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

  const route = validationRouteSummary(item) || "-";
  const routeNode = node.querySelector(".case-route");
  routeNode.textContent = route;
  routeNode.title = [
    route,
    item.validationBackend ? `backend=${item.validationBackend}` : "",
    item.requestedLlmValidationPolicy ? `policy=${item.requestedLlmValidationPolicy}` : "",
  ]
    .filter(Boolean)
    .join(" | ");

  node.querySelector(".case-result").textContent = item.result || "-";
  node.querySelector(".case-dependencies").textContent = item.dependencies || "-";

  if (openIds.has(item.caseId)) {
    node.classList.add("expanded");
    summaryButton.setAttribute("aria-expanded", "true");
    renderCaseDetails(node.querySelector(".case-detail"), item);
  }

  summaryButton.addEventListener("click", () => {
    const isOpen = node.classList.toggle("expanded");
    summaryButton.setAttribute("aria-expanded", isOpen ? "true" : "false");
    if (isOpen) {
      openIds.add(item.caseId);
      renderCaseDetails(node.querySelector(".case-detail"), item);
    } else {
      openIds.delete(item.caseId);
    }
  });

  return node;
}

function renderLlmCases(filtered = null, options = {}) {
  const { force = false } = options;
  const llmCases = filtered !== null ? filtered : llmCasesForRun();
  const nextIds = llmCases.map((item) => item.caseId || "");
  ui.llmCasesCount.textContent = `(${llmCases.length})`;
  const nextKey = caseRenderKey(llmCases, [
    "caseId",
    "status",
    "confidence",
    "skipReason",
    "cached",
    "python",
    "tries",
    "seconds",
    "result",
    "dependencies",
    "pllm",
    "legacy",
    "readpy",
    "validationBackend",
    "validationPath",
    "requestedLlmValidationPolicy",
    "llmValidationRoute",
    "dockerStatus",
    "dockerBypassReason",
    "dockerBypassNote",
    "failureFamily",
    "resultOrigin",
    "debugDir",
    "dockerPlanStatus",
    "dockerPlanPath",
    "authoredDockerfilePath",
    "executedDockerfilePath",
    "dockerBuildCommandPath",
    "dockerRunCommandPath",
    "executedImageRef",
    "imageHandoffVerified",
    "imageInspectPath",
  ]);
  if (!force && state.renderCache.llmCasesKey === nextKey) {
    return;
  }
  if (
    !force &&
    canAppendCases(state.renderCache.llmCaseIds, nextIds) &&
    ui.llmCasesScroll.children.length > 0
  ) {
    const previousScrollTop = ui.llmCasesScroll.scrollTop;
    const fragment = document.createDocumentFragment();
    for (const item of llmCases.slice(state.renderCache.llmCaseIds.length)) {
      fragment.appendChild(buildLLMCaseRow(item, state.openLlmCaseIds));
    }
    ui.llmCasesScroll.appendChild(fragment);
    ui.llmCasesScroll.scrollTop = previousScrollTop;
    state.renderCache.llmCasesKey = nextKey;
    state.renderCache.llmCaseIds = nextIds;
    return;
  }
  state.renderCache.llmCasesKey = nextKey;
  state.renderCache.llmCaseIds = nextIds;
  const previousScrollTop = ui.llmCasesScroll.scrollTop;
  ui.llmCasesScroll.innerHTML = "";
  if (!llmCases.length) {
    state.renderCache.llmCaseIds = [];
    const message = filtered !== null ? "No LLM cases match current filters." : "No LLM cases yet.";
    ui.llmCasesScroll.innerHTML = `<div class="empty-line">${message}</div>`;
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const item of llmCases) {
    fragment.appendChild(buildLLMCaseRow(item, state.openLlmCaseIds));
  }
  ui.llmCasesScroll.appendChild(fragment);
  ui.llmCasesScroll.scrollTop = previousScrollTop;
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
  renderLlmInsights(run);
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
  applyLLMFilters();
  updateSuccessRateDashboard();
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
  const validationBackend = loadout.llm_only_mode
    ? "llm-only"
    : (loadout.validation_backend || state.form.validation_backend || "env");
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
    validation_backend: validationBackend,
    llm_validation_policy: "env-first",
    llm_only_mode: validationBackend === "llm-only",
    loadout_name: loadout.name || "",
  };
  state.selectedLoadoutSlug = loadout.slug;
  syncControlsFromForm();
  requestPreview();
  switchPage("home");
}

function currentConfigPayload() {
  const isLlmOnly = state.form?.validation_backend === "llm-only";
  return {
    ...state.form,
    loadout_name: state.form?.loadout_name || "",
    llm_validation_policy: "env-first",
    llm_only_mode: isLlmOnly,
    // When llm-only is selected, keep the special resolver mode but force Docker
    // validation as the actual backend.
    validation_backend: isLlmOnly ? "docker" : (state.form?.validation_backend || "env"),
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

function setupSSE(runId) {
  // Close existing connection if any
  teardownSSE();

  state.sseConnectionState = "connecting";
  updateSSEStatusIndicator();

  const url = `/api/stream/benchmark/${runId}`;
  const eventSource = new EventSource(url);
  state.sseConnection = eventSource;

  eventSource.onopen = () => {
    state.sseConnectionState = "connected";
    state.sseReconnectAttempts = 0;
    updateSSEStatusIndicator();
  };

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      handleSSEEvent(data);
    } catch (err) {
      console.error("SSE parse error:", err);
    }
  };

  eventSource.onerror = () => {
    state.sseConnectionState = "disconnected";
    updateSSEStatusIndicator();
    eventSource.close();
    state.sseConnection = null;

    // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s (max)
    const delay = Math.min(1000 * Math.pow(2, state.sseReconnectAttempts), 30000);
    state.sseReconnectAttempts++;

    state.sseReconnectTimer = setTimeout(() => {
      if (state.currentRun?.runId === runId) {
        setupSSE(runId);
      }
    }, delay);
  };
}

function teardownSSE() {
  if (state.sseReconnectTimer) {
    clearTimeout(state.sseReconnectTimer);
    state.sseReconnectTimer = null;
  }
  if (state.sseConnection) {
    state.sseConnection.close();
    state.sseConnection = null;
  }
  state.sseConnectionState = "disconnected";
  state.sseReconnectAttempts = 0;
  updateSSEStatusIndicator();
}

function updateSSEStatusIndicator() {
  if (!ui.sseStatusDot || !ui.sseStatusText) return;

  const state_class = state.sseConnectionState; // "connected" | "connecting" | "disconnected"

  // Update visual indicator
  ui.sseStatusDot.className = `sse-status-indicator ${state_class}`;
  ui.sseStatusText.textContent = state_class;

  // Update ARIA live region for screen readers
  if (ui.sseStatusRegion) {
    ui.sseStatusRegion.textContent = `Stream status: ${state_class}`;
  }

  // Show/hide streaming badge
  if (ui.streamingBadge) {
    ui.streamingBadge.style.display = (state_class === "connected") ? "inline-block" : "none";
  }
}

function handleSSEEvent(event) {
  state.ssePendingUpdates.push(event);

  // Schedule batch update if not already scheduled
  if (!state.sseUpdateScheduled) {
    state.sseUpdateScheduled = true;
    requestAnimationFrame(processPendingSSEUpdates);
  }
}

function processPendingSSEUpdates() {
  const updates = state.ssePendingUpdates.splice(0); // Drain queue
  state.sseUpdateScheduled = false;

  if (updates.length === 0) return;

  // Batch process all pending events in single frame
  for (const event of updates) {
    switch (event.type) {
      case "init":
      case "progress":
        updateProgressBar(event.progress);
        break;
      case "status_update":
        updateCaseStatus(event.caseId, event.status);
        addActivityItem(event);
        break;
      case "case_complete":
        updateCaseStatus(event.caseId, event.status);
        addActivityItem(event);
        break;
      case "tier_stats":
        // No longer used - success rates calculated from case data
        break;
      case "heartbeat":
        // No UI update needed, just proves connection alive
        break;
      case "complete":
        handleBenchmarkComplete();
        break;
    }
  }
}

function updateProgressBar(progress) {
  if (!progress) return;
  const {completed, total, percent} = progress;
  ui.progressLabel.textContent = `${completed}/${total}`;
  ui.progressPercent.textContent = `${percent.toFixed(1)}%`;
  ui.progressFill.style.width = `${percent}%`;
}

function updateCaseStatus(caseId, status) {
  // Find existing case row by data-case-id attribute
  const row = ui.casesScroll?.querySelector(`[data-case-id="${caseId}"]`);
  if (row) {
    // Update status badge color class
    const badge = row.querySelector(".status-badge");
    if (badge) {
      badge.className = `status-badge status-${status}`;
      badge.textContent = status;
    }
  } else if (status !== "running") {
    // Append new completed case row (clone template, populate, append)
    appendCaseRow(caseId, status);
  }
}

function appendCaseRow(caseId, status) {
  // This will be implemented when case row template structure is available
  // For now, just log the event
  console.log(`Case ${caseId} completed with status: ${status}`);
}

function addActivityItem(event) {
  if (!ui.recentActivity) return;

  const time = new Date(event.timestamp).toLocaleTimeString();
  const action = event.type === "case_complete"
    ? `${event.caseId}: ${event.status}`
    : `${event.caseId}: ${event.status}`;

  const item = document.createElement("div");
  item.className = "activity-item";
  item.textContent = `• ${time}: ${action}`;

  // Prepend (newest first)
  ui.recentActivity.insertBefore(item, ui.recentActivity.firstChild);

  // Prune to max 10 items (memory leak prevention)
  while (ui.recentActivity.children.length > 10) {
    ui.recentActivity.removeChild(ui.recentActivity.lastChild);
  }
}

function handleBenchmarkComplete() {
  // Update UI to reflect completion
  // Will be enhanced in Task 3 with full integration
  teardownSSE();
}

function updateSuccessRateDashboard() {
  if (!ui.deterministicSuccessValue || !ui.llmSuccessValue) return;

  const run = displayRun();
  if (!run) {
    ui.deterministicSuccessValue.innerHTML = formatSuccessRate(0, 0, 0, 0);
    ui.llmSuccessValue.innerHTML = formatSuccessRate(0, 0, 0, 0);
    return;
  }

  const allCases = Array.isArray(run.completedCases) ? run.completedCases : [];

  // Prefer aggregate counters from the run state. `completedCases` is intentionally
  // capped to the most recent rows for UI responsiveness, so using it directly
  // undercounts large historical runs.
  const deterministicCases = allCases.filter((c) => c.tier === "tier1" || c.tier === "tier2");
  const llmCases = allCases.filter((c) => c.tier === "tier3");

  const detStats = bucketSuccessRateStats(
    run,
    deterministicCases,
    "regularSuccesses",
    "regularFailures",
    "regularSkipped",
  );
  const llmStats = bucketSuccessRateStats(run, llmCases, "llmSuccesses", "llmFailures", "llmSkipped");

  ui.deterministicSuccessValue.innerHTML = formatSuccessRate(
    detStats.succeeded,
    detStats.failed,
    detStats.skipped,
    detStats.total,
  );
  ui.llmSuccessValue.innerHTML = formatSuccessRate(
    llmStats.succeeded,
    llmStats.failed,
    llmStats.skipped,
    llmStats.total,
  );
}

function bucketSuccessRateStats(run, cases, successKey, failureKey, skippedKey) {
  const succeeded = Number.isFinite(Number(run[successKey]))
    ? Number(run[successKey])
    : cases.filter((c) => c.status === "PASS").length;
  const failed = Number.isFinite(Number(run[failureKey]))
    ? Number(run[failureKey])
    : cases.filter((c) => c.status === "FAIL").length;
  const skipped = Number.isFinite(Number(run[skippedKey]))
    ? Number(run[skippedKey])
    : cases.filter((c) => c.status === "SKIP").length;
  const total = succeeded + failed + skipped;
  return {
    succeeded,
    failed,
    skipped,
    total,
  };
}

function formatSuccessRate(succeeded, failed, skipped, total) {
  return `
    <span style="color: #50fa7b;">Successes: <span style="font-weight: bold;">${succeeded}</span></span>
    <span style="color: #6272a4;"> / </span>
    <span style="color: #ff5555;">Failures: <span style="font-weight: bold;">${failed}</span></span>
    <span style="color: #6272a4;"> / </span>
    <span style="color: #f1fa8c;">Skipped: <span style="font-weight: bold;">${skipped}</span></span>
    <span style="color: #6272a4;"> / </span>
    <span style="color: #f8f8f2;">Total: <span style="font-weight: bold;">${total}</span></span>
  `;
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
    if (state.activePage === "run") {
      renderRunPage();
    }
    if (state.activePage === "doctor") {
      renderDoctor();
    }

    // Setup SSE when runId becomes available for active run
    const newRunId = state.currentRun?.runId || "";
    if (newRunId && newRunId !== previousRunId && isRunActive(state.currentRun)) {
      setupSSE(newRunId);
    }

    // Teardown SSE when run becomes inactive
    if (previousRunId && !isRunActive(state.currentRun) && state.sseConnection) {
      teardownSSE();
    }

    if (
      previousRunId &&
      (previousRunId !== newRunId || previousStatus !== (state.currentRun?.status || "")) &&
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

      // SSE will be setup automatically by pollStatus once runId is available
    } catch (error) {
      alert(error.message);
    }
  });
  ui.stopButton.addEventListener("click", async () => {
    try {
      // Teardown SSE connection before stopping
      teardownSSE();

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
            <div class="terminal-badge">APDR Benchmark Dashboard</div>
            <section class="terminal-section">
              <div class="terminal-divider"><span>Server Stopped</span></div>
              <div class="warning-title">APDR Bench</div>
              <div class="warning-copy">The local web server is stopping. You can close this tab.</div>
            </section>
          </section>
        </main>
      `;
    } catch (error) {
      alert(error.message);
    }
  });

  ui.runHomeButton.addEventListener("click", () => {
    switchPage("home");
  });
  ui.runStopButton.addEventListener("click", async () => {
    try {
      // Teardown SSE connection before stopping
      teardownSSE();

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
        validation_backend: state.form.validation_backend,
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
        validation_backend: state.form.validation_backend,
      });
      state.doctor = payload.doctor;
      renderDoctor();
    } catch (error) {
      alert(error.message);
    }
  });
}

async function initialize() {
  // Start performance timer
  console.time("ui-interactive");
  const initialPage = pathToPage(window.location.pathname);

  setupDropdowns();
  setupLLMFilters();
  setupDeterministicFilters();
  wireTabs();
  wireHomeControls();
  wireConfigure();
  wireLoadouts();
  wireDoctor();

  // FAST PATH: Essential data only for <500ms interactive
  const payload = await fetchJson("/api/bootstrap");
  state.app = payload.app;
  state.form = payload.defaultConfig;
  state.preview = payload.homePreview;
  state.currentRun = payload.currentRun;
  state.modelConfigs = payload.modelConfigs || {};
  state.doctor = payload.doctor;

  // Populate essential UI immediately
  populateToolSelect();
  syncControlsFromForm();
  renderHome();
  if (initialPage === "run") {
    renderRunPage();
  } else if (initialPage === "configure") {
    renderConfigure();
  } else if (initialPage === "doctor") {
    renderDoctor();
  }
  switchPage(initialPage, { pushHistory: false, replaceHistory: true });

  // Initialize SSE status indicator
  updateSSEStatusIndicator();

  // Initialize success rate dashboard
  updateSuccessRateDashboard();

  // Mark UI as interactive
  console.timeEnd("ui-interactive");

  state.pollTimer = window.setInterval(pollStatus, 1000);

  // DEFERRED PATH: Load heavy data in background (doesn't block interaction)
  setTimeout(async () => {
    try {
      // Load loadouts
      state.loadouts = payload.loadouts || [];
      state.selectedLoadoutSlug = state.loadouts[0]?.slug || "";
      renderLoadouts();

      // Load run history after first paint; this can be expensive on large repos.
      await refreshRuns();
    } catch (err) {
      console.error("Deferred load failed:", err);
      // Non-fatal: UI still functional, just missing history/loadouts
    }
  }, 100); // Defer 100ms to ensure UI renders first
}

initialize().catch((error) => {
  document.body.innerHTML = `
    <main class="terminal-shell">
      <section class="terminal-frame">
        <div class="terminal-badge">APDR Benchmark Dashboard</div>
        <section class="terminal-section">
          <div class="terminal-divider"><span>Startup Error</span></div>
          <div class="warning-title">Unable to start the web UI</div>
          <div class="warning-copy">${escapeHtml(error.message)}</div>
        </section>
      </section>
    </main>
  `;
});
