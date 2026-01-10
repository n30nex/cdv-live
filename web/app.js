const state = {
  nodes: new Map(),
  links: new Map(),
  nodesSummary: [],
  nodeNames: new Map(),
  packets: [],
  filteredPackets: [],
  paused: false,
  selectedId: null,
  lastUpdate: null,
  connection: "connecting",
  socket: null,
  refreshTimer: null,
  drawerRequestId: 0,
  activeNodeId: null,
  selectedNodeId: null,
  autoPaused: false,
  selectionRequestId: 0,
  selectedRoutes: [],
  selectedLinks: [],
  graphView: {
    nodes: new Map(),
    links: new Map(),
  },
  filters: {
    window: 3600,
    portnum: "",
    channel: "",
    gateway: "",
    search: "",
  },
};

const BROADCAST_ID = 0xffffffff;
const LINK_FLASH_MS = 900;
const LINK_FADE_MS = 8000;
const LINK_HEAT_GAIN = 1;
const LINK_HEAT_MAX = 6;
const LINK_HEAT_HALF_LIFE_MS = 30000;
const PULSE_MS = 1200;
const ROUTING_PORTNUM = 5;
const TRACEROUTE_PORTNUM = 70;
const ROUTE_STEP_MS = 360;
const ROUTE_FADE_MS = 20000;
const ROUTE_COLOR_FORWARD = "#33ff79";
const ROUTE_COLOR_RETURN = "#ff3b3b";
const SEND_FLASH_MS = 5000;
const RECEIVE_FLASH_MS = 6000;
const NODE_HEAT_GAIN = 0.7;
const NODE_HEAT_MAX = 4.5;
const NODE_HEAT_HALF_LIFE_MS = 3400;
const ACTIVITY_DECAY_MS = 4200;
const ACTIVITY_SPIKE_DECAY_MS = 1100;
const RIPPLE_RING_STAGGER_MS = 70;
const RIPPLE_RING_SPACING = 6;
const TEXT_BUBBLE_MS = 30000;
const TEXT_BUBBLE_MAX = 24;
const TEXT_BUBBLE_MAX_LINES = 3;
const TEXT_BUBBLE_MAX_WIDTH = 220;
const TEXT_BUBBLE_PADDING_X = 10;
const TEXT_BUBBLE_PADDING_Y = 6;
const TEXT_BUBBLE_LINE_HEIGHT = 14;
const TEXT_BUBBLE_MAX_CHARS = 160;
const PANEL_SNAP_PX = 12;
const PANEL_MIN_WIDTH = 240;
const PANEL_MIN_HEIGHT = 160;
const PANEL_LAYOUT_KEY = "meshviz.panelLayout.v1";
const PACKET_RENDER_LIMIT = 300;
const FEED_FOLLOW_THRESHOLD_PX = 6;
const HOVER_INDEX_REFRESH_MS = 120;
const HOVER_MAX_RADIUS = 26;
const PACKET_FLUSH_BUDGET_MS = 6;
const STATS_UPDATE_INTERVAL_MS = 600;
const FETCH_TIMEOUT_MS = 10000;

const palette = [
  "#5ad8c8",
  "#f7c94b",
  "#f77f6b",
  "#7aa9ff",
  "#9bde7d",
  "#ff9f68",
  "#d27bff",
  "#46d3ff",
];

const urlParams = new URLSearchParams(window.location.search);
const stressConfig = getStressConfig(urlParams);
const perfStats = initPerfStats(urlParams, stressConfig);

const brokerValue = document.getElementById("brokerValue");
const topicValue = document.getElementById("topicValue");
const liveStatus = document.getElementById("liveStatus");
const pauseBtn = document.getElementById("pauseBtn");
const windowSelect = document.getElementById("windowSelect");
const portFilter = document.getElementById("portFilter");
const channelFilter = document.getElementById("channelFilter");
const gatewayFilter = document.getElementById("gatewayFilter");
const nodeSearch = document.getElementById("nodeSearch");
const packetRows = document.getElementById("packetRows");
const packetTableWrap = packetRows ? packetRows.closest(".table-wrap") : null;
const packetCount = document.getElementById("packetCount");
const packetDetails = document.getElementById("packetDetails");
const selectedMeta = document.getElementById("selectedMeta");
const detailType = document.getElementById("detailType");
const graphStats = document.getElementById("graphStats");
const graphTime = document.getElementById("graphTime");
const legend = document.getElementById("legend");
const metricPpm = document.getElementById("metricPpm");
const metricActive = document.getElementById("metricActive");
const metricPort = document.getElementById("metricPort");
const metricRssi = document.getElementById("metricRssi");
const metricSnr = document.getElementById("metricSnr");
const nodeRows = document.getElementById("nodeRows");
const nodeCount = document.getElementById("nodeCount");
const nodeDrawer = document.getElementById("nodeDrawer");
const drawerTitle = document.getElementById("drawerTitle");
const drawerSubtitle = document.getElementById("drawerSubtitle");
const drawerIdentity = document.getElementById("drawerIdentity");
const drawerPorts = document.getElementById("drawerPorts");
const drawerPeers = document.getElementById("drawerPeers");
const drawerPackets = document.getElementById("drawerPackets");
const drawerClose = document.getElementById("drawerClose");
const graphTooltip = document.getElementById("graphTooltip");
const panelToggleAll = document.getElementById("panelToggleAll");
const panelControls = document.getElementById("panelControls");
const panelControlsToggle = document.getElementById("panelControlsToggle");
const panelControlsClose = document.getElementById("panelControlsClose");
const panelControlsList = document.getElementById("panelControlsList");
const panelAllToggle = document.getElementById("panelAllToggle");
const panelToggles = document.querySelectorAll(".panel-toggle");
const panelMap = new Map();
const feedToggle = document.getElementById("feedToggle");
let feedPanel = null;
const panelOverlay = document.querySelector(".graph-overlay");
const panelLayouts = new Map();
const draggablePanels = panelOverlay ? Array.from(panelOverlay.querySelectorAll(".panel")) : [];
let panelZ = 10;
let panelLayoutTimer = null;
const packetRenderState = { query: "", ids: [] };
const packetQueue = [];
let packetFlushPending = false;
const MAX_PACKETS_PER_FLUSH = 200;
let lastStatsUpdate = 0;
const loadingOverlay = document.getElementById("loadingOverlay");
const loadingStatus = document.getElementById("loadingStatus");
let loadingHidden = false;

document.querySelectorAll(".panel[data-panel]").forEach((panel) => {
  panelMap.set(panel.dataset.panel, panel);
});
feedPanel = panelMap.get("feed") || null;

pauseBtn.addEventListener("click", () => {
  const nextPaused = !state.paused;
  setPaused(nextPaused, { auto: false });
  if (!nextPaused && state.selectedNodeId !== null && state.selectedNodeId !== undefined) {
    clearNodeSelection();
  }
});

windowSelect.addEventListener("change", () => {
  state.filters.window = Number(windowSelect.value);
  refreshAll();
});

portFilter.addEventListener("change", () => {
  state.filters.portnum = portFilter.value;
  refreshAll();
});

channelFilter.addEventListener("change", () => {
  state.filters.channel = channelFilter.value;
  refreshAll();
});

gatewayFilter.addEventListener(
  "input",
  debounce(() => {
    state.filters.gateway = gatewayFilter.value.trim();
    refreshAll();
  }, 300),
);

nodeSearch.addEventListener(
  "input",
  debounce(() => {
    state.filters.search = nodeSearch.value.trim();
    renderPackets();
    renderNodes();
  }, 150),
);

if (packetRows) {
  packetRows.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-id]");
    if (!row) return;
    const id = Number(row.dataset.id);
    const packet = state.packets.find((item) => item.id === id);
    if (packet) {
      setSelected(packet);
    }
  });
}

if (nodeRows) {
  nodeRows.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-id]");
    if (!row) return;
    const nodeId = Number(row.dataset.id);
    if (!Number.isNaN(nodeId) && nodeId !== BROADCAST_ID) {
      selectNode(nodeId);
    }
  });
}

drawerClose.addEventListener("click", () => {
  closeNodeDrawer();
});

function setLoadingHidden(hidden) {
  if (!loadingOverlay || loadingHidden === hidden) {
    return;
  }
  loadingHidden = hidden;
  loadingOverlay.classList.toggle("is-hidden", hidden);
}

function updateLoadingState() {
  if (!loadingOverlay || loadingHidden) {
    return;
  }
  const hasNodes = state.nodesSummary.length > 0;
  const hasTraffic = state.packets.length > 0 || state.links.size > 0;
  if (hasNodes || hasTraffic) {
    setLoadingHidden(true);
    return;
  }
  if (!loadingStatus) {
    return;
  }
  if (state.connection === "disconnected") {
    loadingStatus.textContent = "Disconnected. Retrying...";
  } else if (state.paused) {
    loadingStatus.textContent = "Paused. Waiting for traffic...";
  } else if (state.connection === "live") {
    loadingStatus.textContent = "Connected. Waiting for traffic...";
  } else {
    loadingStatus.textContent = "Connecting to live feed...";
  }
}

function updatePanelToggleAll() {
  if (!panelToggleAll) return;
  const hidden = document.body.classList.contains("panels-hidden");
  panelToggleAll.textContent = hidden ? "Show Panels" : "Hide Panels";
}

function syncPanelState() {
  const feedVisible = feedPanel ? !feedPanel.classList.contains("is-hidden") : false;
  document.body.classList.toggle("feed-visible", feedVisible);
  if (!feedVisible) {
    document.body.classList.remove("feed-expanded");
  }
  updateFeedToggle();
  clampFloatingPanels();
}

function syncPanelsHiddenClass() {
  const anyVisible = Array.from(panelMap.values()).some(
    (panel) => !panel.classList.contains("is-hidden"),
  );
  document.body.classList.toggle("panels-hidden", !anyVisible);
  updatePanelControls();
  updatePanelToggleAll();
}

function setAllPanelsHidden(hidden) {
  panelMap.forEach((panel) => {
    panel.classList.toggle("is-hidden", hidden);
  });
  document.body.classList.toggle("panels-hidden", hidden);
  syncPanelState();
  updatePanelControls();
  updatePanelToggleAll();
}

if (panelToggleAll) {
  panelToggleAll.addEventListener("click", () => {
    const hidden = !document.body.classList.contains("panels-hidden");
    setAllPanelsHidden(hidden);
  });
}

function updateFeedToggle() {
  if (!feedToggle) return;
  const expanded = document.body.classList.contains("feed-expanded");
  feedToggle.textContent = expanded ? "Collapse" : "Expand";
}

if (feedToggle) {
  feedToggle.addEventListener("click", () => {
    document.body.classList.toggle("feed-expanded");
    updateFeedToggle();
    clampFloatingPanels();
  });
  updateFeedToggle();
}

function setPanelControlsCollapsed(collapsed) {
  if (!panelControls) return;
  panelControls.classList.toggle("is-collapsed", collapsed);
  if (panelControlsToggle) {
    panelControlsToggle.setAttribute("aria-expanded", String(!collapsed));
  }
}

function updatePanelControls() {
  if (!panelControlsList) return;
  const inputs = panelControlsList.querySelectorAll("input[data-panel]");
  let visibleCount = 0;
  inputs.forEach((input) => {
    const panel = panelMap.get(input.dataset.panel);
    const isVisible = panel ? !panel.classList.contains("is-hidden") : false;
    input.checked = isVisible;
    if (isVisible) {
      visibleCount += 1;
    }
  });
  if (panelAllToggle) {
    const total = panelMap.size;
    panelAllToggle.checked = total > 0 && visibleCount === total;
    panelAllToggle.indeterminate = visibleCount > 0 && visibleCount < total;
  }
}

function buildPanelControls() {
  if (!panelControlsList) return;
  panelControlsList.innerHTML = "";
  const order = ["graph", "metrics", "nodes", "details", "feed"];
  const added = new Set();
  const addPanelToggle = (panelId, panel) => {
    if (!panel) return;
    const label = panel.dataset.panelLabel || "Panel";
    const wrapper = document.createElement("label");
    wrapper.className = "panel-switch";
    const input = document.createElement("input");
    input.type = "checkbox";
    input.dataset.panel = panelId;
    input.checked = !panel.classList.contains("is-hidden");
    const text = document.createElement("span");
    text.textContent = label;
    wrapper.appendChild(input);
    wrapper.appendChild(text);
    input.addEventListener("change", () => {
      panel.classList.toggle("is-hidden", !input.checked);
      syncPanelState();
      syncPanelsHiddenClass();
    });
    panelControlsList.appendChild(wrapper);
  };

  order.forEach((panelId) => {
    const panel = panelMap.get(panelId);
    if (panel) {
      addPanelToggle(panelId, panel);
      added.add(panelId);
    }
  });
  panelMap.forEach((panel, panelId) => {
    if (added.has(panelId)) return;
    addPanelToggle(panelId, panel);
  });
  updatePanelControls();
}

if (panelControlsToggle) {
  panelControlsToggle.addEventListener("click", () => {
    const collapsed = panelControls ? panelControls.classList.contains("is-collapsed") : false;
    setPanelControlsCollapsed(!collapsed);
  });
}

if (panelControlsClose) {
  panelControlsClose.addEventListener("click", () => {
    setPanelControlsCollapsed(true);
  });
}

if (panelAllToggle) {
  panelAllToggle.addEventListener("change", () => {
    setAllPanelsHidden(!panelAllToggle.checked);
  });
}

panelToggles.forEach((button) => {
  button.addEventListener("click", () => {
    const panelId = button.dataset.panel;
    const panel = panelMap.get(panelId);
    if (!panel) return;
    panel.classList.toggle("is-hidden");
    syncPanelState();
    syncPanelsHiddenClass();
  });
});

buildPanelControls();
setAllPanelsHidden(true);
initPanelDragging();

function clampValue(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function getOverlayRect() {
  if (!panelOverlay) return null;
  return panelOverlay.getBoundingClientRect();
}

function panelMinSize(panel) {
  const minWidth = Number(panel.dataset.minWidth) || PANEL_MIN_WIDTH;
  const minHeight = Number(panel.dataset.minHeight) || PANEL_MIN_HEIGHT;
  return { minWidth, minHeight };
}

function ensurePanelGrip(panel) {
  const header = panel.querySelector(".panel-header");
  if (!header) return null;
  let actions = header.querySelector(".panel-actions");
  if (!actions) {
    actions = document.createElement("div");
    actions.className = "panel-actions";
    const headerChildren = Array.from(header.children);
    if (headerChildren.length > 1) {
      const fragment = document.createDocumentFragment();
      headerChildren.slice(1).forEach((child) => fragment.appendChild(child));
      actions.appendChild(fragment);
    }
    header.appendChild(actions);
  }
  let grip = actions.querySelector(".panel-grip");
  if (!grip) {
    grip = document.createElement("button");
    grip.type = "button";
    grip.className = "panel-grip";
    grip.setAttribute("aria-label", "Drag panel");
    actions.prepend(grip);
  }
  return grip;
}

function ensurePanelResize(panel) {
  let resizer = panel.querySelector(".panel-resize");
  if (!resizer) {
    resizer = document.createElement("div");
    resizer.className = "panel-resize";
    resizer.setAttribute("aria-hidden", "true");
    panel.appendChild(resizer);
  }
  return resizer;
}

function bringPanelToFront(panel) {
  panelZ += 1;
  panel.style.zIndex = String(panelZ);
}

function makePanelFloating(panel) {
  if (!panelOverlay || panel.classList.contains("panel-floating")) return;
  const overlayRect = getOverlayRect();
  if (!overlayRect) return;
  const rect = panel.getBoundingClientRect();
  panel.classList.add("panel-floating");
  panel.style.left = `${rect.left - overlayRect.left}px`;
  panel.style.top = `${rect.top - overlayRect.top}px`;
  panel.style.width = `${rect.width}px`;
  panel.style.height = `${rect.height}px`;
  panel.style.right = "auto";
  panel.style.bottom = "auto";
}

function updatePanelLayout(panel) {
  if (!panelOverlay) return;
  const panelId = panel.dataset.panel;
  if (!panelId) return;
  if (!panel.classList.contains("panel-floating")) {
    panelLayouts.delete(panelId);
    schedulePanelLayoutSave();
    return;
  }
  const overlayRect = getOverlayRect();
  if (!overlayRect) return;
  const rect = panel.getBoundingClientRect();
  panelLayouts.set(panelId, {
    left: rect.left - overlayRect.left,
    top: rect.top - overlayRect.top,
    width: rect.width,
    height: rect.height,
    z: Number(panel.style.zIndex) || 0,
  });
  schedulePanelLayoutSave();
}

function clampPanelToOverlay(panel) {
  if (!panelOverlay) return;
  const overlayRect = getOverlayRect();
  if (!overlayRect) return;
  const rect = panel.getBoundingClientRect();
  const { minWidth, minHeight } = panelMinSize(panel);
  const maxWidth = overlayRect.width;
  const maxHeight = overlayRect.height;
  const safeMinWidth = Math.min(minWidth, maxWidth);
  const safeMinHeight = Math.min(minHeight, maxHeight);
  let width = clampValue(rect.width, safeMinWidth, maxWidth);
  let height = clampValue(rect.height, safeMinHeight, maxHeight);
  let left = clampValue(rect.left - overlayRect.left, 0, overlayRect.width - width);
  let top = clampValue(rect.top - overlayRect.top, 0, overlayRect.height - height);
  panel.style.width = `${width}px`;
  panel.style.height = `${height}px`;
  panel.style.left = `${left}px`;
  panel.style.top = `${top}px`;
  updatePanelLayout(panel);
}

function clampFloatingPanels() {
  if (!panelOverlay || !draggablePanels.length) return;
  draggablePanels.forEach((panel) => {
    if (panel.classList.contains("panel-floating")) {
      clampPanelToOverlay(panel);
    }
  });
}

function schedulePanelLayoutSave() {
  if (panelLayoutTimer) return;
  panelLayoutTimer = window.setTimeout(() => {
    const payload = {};
    panelLayouts.forEach((value, key) => {
      payload[key] = value;
    });
    try {
      localStorage.setItem(PANEL_LAYOUT_KEY, JSON.stringify(payload));
    } catch (err) {
      // Ignore storage errors.
    }
    panelLayoutTimer = null;
  }, 120);
}

function loadPanelLayouts() {
  if (!panelOverlay) return;
  let raw = null;
  try {
    raw = localStorage.getItem(PANEL_LAYOUT_KEY);
  } catch (err) {
    return;
  }
  if (!raw) return;
  let saved = null;
  try {
    saved = JSON.parse(raw);
  } catch (err) {
    return;
  }
  if (!saved || typeof saved !== "object") return;
  const overlayRect = getOverlayRect();
  if (!overlayRect) return;
  Object.entries(saved).forEach(([panelId, layout]) => {
    const panel = panelMap.get(panelId);
    if (!panel || !panelOverlay.contains(panel)) return;
    if (!layout || typeof layout !== "object") return;
    const { minWidth, minHeight } = panelMinSize(panel);
    const maxWidth = overlayRect.width;
    const maxHeight = overlayRect.height;
    const safeMinWidth = Math.min(minWidth, maxWidth);
    const safeMinHeight = Math.min(minHeight, maxHeight);
    let width = Number(layout.width);
    let height = Number(layout.height);
    let left = Number(layout.left);
    let top = Number(layout.top);
    if (!Number.isFinite(width) || !Number.isFinite(height)) return;
    if (!Number.isFinite(left) || !Number.isFinite(top)) return;
    width = clampValue(width, safeMinWidth, maxWidth);
    height = clampValue(height, safeMinHeight, maxHeight);
    left = clampValue(left, 0, overlayRect.width - width);
    top = clampValue(top, 0, overlayRect.height - height);
    panel.classList.add("panel-floating");
    panel.style.left = `${left}px`;
    panel.style.top = `${top}px`;
    panel.style.width = `${width}px`;
    panel.style.height = `${height}px`;
    panel.style.right = "auto";
    panel.style.bottom = "auto";
    if (layout.z) {
      panel.style.zIndex = String(layout.z);
      panelZ = Math.max(panelZ, layout.z);
    }
    panelLayouts.set(panelId, {
      left,
      top,
      width,
      height,
      z: Number(panel.style.zIndex) || 0,
    });
  });
}

function startPanelDrag(event, panel) {
  if (event.button !== 0) return;
  if (!panelOverlay) return;
  event.preventDefault();
  makePanelFloating(panel);
  bringPanelToFront(panel);
  panel.classList.add("is-dragging");
  document.body.classList.add("panel-dragging");
  const overlayRect = getOverlayRect();
  if (!overlayRect) return;
  const rect = panel.getBoundingClientRect();
  const startLeft = rect.left - overlayRect.left;
  const startTop = rect.top - overlayRect.top;
  const startX = event.clientX;
  const startY = event.clientY;
  const maxLeft = Math.max(0, overlayRect.width - rect.width);
  const maxTop = Math.max(0, overlayRect.height - rect.height);

  const handleMove = (moveEvent) => {
    const dx = moveEvent.clientX - startX;
    const dy = moveEvent.clientY - startY;
    let nextLeft = clampValue(startLeft + dx, 0, maxLeft);
    let nextTop = clampValue(startTop + dy, 0, maxTop);
    if (Math.abs(nextLeft) <= PANEL_SNAP_PX) {
      nextLeft = 0;
    } else if (Math.abs(nextLeft - maxLeft) <= PANEL_SNAP_PX) {
      nextLeft = maxLeft;
    }
    if (Math.abs(nextTop) <= PANEL_SNAP_PX) {
      nextTop = 0;
    } else if (Math.abs(nextTop - maxTop) <= PANEL_SNAP_PX) {
      nextTop = maxTop;
    }
    panel.style.left = `${nextLeft}px`;
    panel.style.top = `${nextTop}px`;
  };

  const handleUp = () => {
    window.removeEventListener("pointermove", handleMove);
    panel.classList.remove("is-dragging");
    document.body.classList.remove("panel-dragging");
    clampPanelToOverlay(panel);
  };

  window.addEventListener("pointermove", handleMove);
  window.addEventListener("pointerup", handleUp, { once: true });
  window.addEventListener("pointercancel", handleUp, { once: true });
}

function startPanelResize(event, panel) {
  if (event.button !== 0) return;
  if (!panelOverlay) return;
  event.preventDefault();
  makePanelFloating(panel);
  bringPanelToFront(panel);
  panel.classList.add("is-resizing");
  document.body.classList.add("panel-dragging");
  const overlayRect = getOverlayRect();
  if (!overlayRect) return;
  const rect = panel.getBoundingClientRect();
  const startWidth = rect.width;
  const startHeight = rect.height;
  const startLeft = rect.left - overlayRect.left;
  const startTop = rect.top - overlayRect.top;
  const startX = event.clientX;
  const startY = event.clientY;
  const { minWidth, minHeight } = panelMinSize(panel);
  const maxWidth = Math.max(0, overlayRect.width - startLeft);
  const maxHeight = Math.max(0, overlayRect.height - startTop);
  const safeMinWidth = Math.min(minWidth, maxWidth || minWidth);
  const safeMinHeight = Math.min(minHeight, maxHeight || minHeight);

  const handleMove = (moveEvent) => {
    const dx = moveEvent.clientX - startX;
    const dy = moveEvent.clientY - startY;
    let width = clampValue(startWidth + dx, safeMinWidth, maxWidth || safeMinWidth);
    let height = clampValue(startHeight + dy, safeMinHeight, maxHeight || safeMinHeight);
    if (Math.abs(startLeft + width - overlayRect.width) <= PANEL_SNAP_PX) {
      width = overlayRect.width - startLeft;
    }
    if (Math.abs(startTop + height - overlayRect.height) <= PANEL_SNAP_PX) {
      height = overlayRect.height - startTop;
    }
    panel.style.width = `${width}px`;
    panel.style.height = `${height}px`;
  };

  const handleUp = () => {
    window.removeEventListener("pointermove", handleMove);
    panel.classList.remove("is-resizing");
    document.body.classList.remove("panel-dragging");
    clampPanelToOverlay(panel);
  };

  window.addEventListener("pointermove", handleMove);
  window.addEventListener("pointerup", handleUp, { once: true });
  window.addEventListener("pointercancel", handleUp, { once: true });
}

function initPanelDragging() {
  if (!panelOverlay || !draggablePanels.length) return;
  draggablePanels.forEach((panel) => {
    const grip = ensurePanelGrip(panel);
    const resizer = ensurePanelResize(panel);
    if (grip) {
      grip.addEventListener("pointerdown", (event) => startPanelDrag(event, panel));
    }
    if (resizer) {
      resizer.addEventListener("pointerdown", (event) => startPanelResize(event, panel));
    }
    panel.addEventListener("pointerdown", (event) => {
      if (
        event.target.closest(
          ".panel-grip, .panel-toggle, .panel-resize, button, input, select, textarea",
        )
      ) {
        return;
      }
      bringPanelToFront(panel);
    });
  });
  loadPanelLayouts();
  window.addEventListener("resize", () => {
    clampFloatingPanels();
  });
}

function debounce(fn, delay) {
  let timer = null;
  return (...args) => {
    if (timer) {
      clearTimeout(timer);
    }
    timer = setTimeout(() => fn(...args), delay);
  };
}

function setPaused(paused, options = {}) {
  const isAuto = options.auto === true;
  state.paused = paused;
  state.autoPaused = isAuto ? paused : false;
  pauseBtn.textContent = paused ? "Resume" : "Pause";
  pauseBtn.classList.toggle("active", paused);
  if (!paused && state.connection === "disconnected") {
    connectWs();
  }
  updateLiveStatus();
}

function updateLiveStatus() {
  liveStatus.classList.remove("pill-muted", "live");
  if (state.connection === "disconnected") {
    liveStatus.textContent = "Disconnected";
    liveStatus.classList.add("pill-muted");
  } else if (state.paused) {
    liveStatus.textContent = "Paused";
    liveStatus.classList.add("pill-muted");
  } else if (state.connection === "live") {
    liveStatus.textContent = "Live";
    liveStatus.classList.add("live");
  } else {
    liveStatus.textContent = "Connecting";
    liveStatus.classList.add("pill-muted");
  }
  updateLoadingState();
}

function colorForPort(portnum) {
  if (portnum === null || portnum === undefined) {
    return "#5c6b7a";
  }
  return palette[Math.abs(Number(portnum)) % palette.length];
}

function colorForNode(nodeId) {
  if (nodeId === null || nodeId === undefined) {
    return "#5c6b7a";
  }
  if (nodeId === BROADCAST_ID) {
    return "#8da0b2";
  }
  const value = Number(nodeId);
  if (!Number.isFinite(value)) {
    return palette[0];
  }
  const hashed = (value ^ (value >>> 16)) >>> 0;
  return palette[hashed % palette.length];
}

function hexToRgb(hex) {
  if (typeof hex !== "string") {
    return null;
  }
  const normalized = hex.replace("#", "");
  const full = normalized.length === 3
    ? normalized.split("").map((char) => char + char).join("")
    : normalized;
  if (full.length !== 6) {
    return null;
  }
  const value = Number.parseInt(full, 16);
  if (Number.isNaN(value)) {
    return null;
  }
  return {
    r: (value >> 16) & 255,
    g: (value >> 8) & 255,
    b: value & 255,
  };
}

function rgbaFromHex(hex, alpha) {
  const rgb = hexToRgb(hex);
  if (!rgb) {
    return `rgba(255, 255, 255, ${alpha})`;
  }
  return `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${alpha})`;
}

function formatTime(epoch) {
  if (!epoch) return "--:--:--";
  const date = new Date(epoch * 1000);
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatNodeId(nodeId) {
  if (nodeId === null || nodeId === undefined) return "--";
  return `!${nodeId.toString(16).padStart(8, "0")}`;
}

function escapeHtml(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function nodeLabelFromInfo(nodeId, info) {
  if (nodeId === null || nodeId === undefined) return "--";
  if (nodeId === BROADCAST_ID) return "broadcast";
  if (info) {
    return info.short_name || info.long_name || formatNodeId(nodeId);
  }
  return formatNodeId(nodeId);
}

function nodeLabel(nodeId) {
  if (nodeId === null || nodeId === undefined) return "--";
  if (nodeId === BROADCAST_ID) return "broadcast";
  const label = state.nodeNames.get(nodeId);
  return label || formatNodeId(nodeId);
}

function payloadSize(packet) {
  if (!packet.payload_b64) return 0;
  return Math.floor((packet.payload_b64.length * 3) / 4);
}

function previewText(packet) {
  if (packet.text) return packet.text.slice(0, 80);
  if (packet.details && packet.details.text) return String(packet.details.text).slice(0, 80);
  if (packet.details && packet.details.user && packet.details.user.long_name) {
    return `Nodeinfo: ${packet.details.user.long_name}`;
  }
  return packet.portname || "Unknown";
}

function formatSignal(packet) {
  const rssi = packet.rssi;
  const snr = packet.snr;
  if (rssi === null || rssi === undefined) {
    if (snr === null || snr === undefined) {
      return "--";
    }
    return `-- dBm / ${snr} dB`;
  }
  if (snr === null || snr === undefined) {
    return `${rssi} dBm / -- dB`;
  }
  return `${rssi} dBm / ${snr} dB`;
}

function formatHops(packet) {
  const start = packet.hop_start;
  const limit = packet.hop_limit;
  if (start === null || start === undefined || limit === null || limit === undefined) {
    return "--";
  }
  const hops = start - limit;
  if (!Number.isFinite(hops) || hops < 0) {
    return "--";
  }
  return String(hops);
}

function packetMatchesSearch(packet, query) {
  if (!query) return true;
  const text = query.toLowerCase();
  const fields = [
    packet.from_label,
    packet.to_label,
    packet.portname,
    packet.text,
    packet.details && packet.details.text,
    packet.gateway_id,
    packet.portnum !== null && packet.portnum !== undefined ? String(packet.portnum) : null,
    packet.from_id !== null && packet.from_id !== undefined ? formatNodeId(packet.from_id) : null,
    packet.to_id !== null && packet.to_id !== undefined ? formatNodeId(packet.to_id) : null,
  ];
  return fields
    .filter((value) => value !== null && value !== undefined)
    .some((value) => String(value).toLowerCase().includes(text));
}

function packetMatchesFilters(packet) {
  if (state.filters.portnum !== "") {
    if (packet.portnum === null || packet.portnum === undefined) {
      return false;
    }
    if (String(packet.portnum) !== state.filters.portnum) {
      return false;
    }
  }
  if (state.filters.channel !== "") {
    if (packet.channel === null || packet.channel === undefined) {
      return false;
    }
    if (Number(packet.channel) !== Number(state.filters.channel)) {
      return false;
    }
  }
  if (state.filters.gateway) {
    if ((packet.gateway_id || "") !== state.filters.gateway) {
      return false;
    }
  }
  return true;
}

function nodeMatchesSearch(node, query) {
  if (!query) return true;
  const text = query.toLowerCase();
  const label = nodeLabelFromInfo(node.node_id, node);
  return (
    String(node.node_id).includes(text) ||
    formatNodeId(node.node_id).toLowerCase().includes(text) ||
    label.toLowerCase().includes(text)
  );
}

function normalizePacket(packet) {
  const normalized = { ...packet };
  if (!normalized.created_at) {
    normalized.created_at = Math.floor(Date.now() / 1000);
  }
  if (!normalized.from_label) {
    normalized.from_label = nodeLabel(normalized.from_id);
  }
  if (!normalized.to_label) {
    normalized.to_label =
      normalized.to_id === BROADCAST_ID ? "broadcast" : nodeLabel(normalized.to_id);
  }
  return normalized;
}

function updateLegend(linkMap) {
  const counts = new Map();
  linkMap.forEach((link) => {
    const key = `${link.portnum}:${link.portname || "Unknown"}`;
    if (!counts.has(key)) {
      counts.set(key, { portnum: link.portnum, name: link.portname || "Unknown", count: 0 });
    }
    counts.get(key).count += link.count || 0;
  });

  const items = Array.from(counts.values())
    .sort((a, b) => b.count - a.count)
    .slice(0, 6);

  const baseItems = [
    { name: "Route out", color: ROUTE_COLOR_FORWARD, swatchClass: "legend-route-forward" },
    { name: "Route return", color: ROUTE_COLOR_RETURN, swatchClass: "legend-route-return" },
    { name: "RF link", color: "rgba(95, 227, 208, 0.7)", swatchClass: "legend-rf" },
    { name: "Broadcast link", color: "rgba(141, 160, 178, 0.4)", swatchClass: "legend-broadcast" },
  ];

  const baseHtml = baseItems
    .map((item) => {
      return `<span class="legend-item"><span class="legend-swatch ${item.swatchClass}" style="background:${item.color}"></span>${escapeHtml(item.name)}</span>`;
    })
    .join("");

  const portHtml = items
    .map((item) => {
      const color = colorForPort(item.portnum);
      return `<span class="legend-item"><span class="legend-swatch" style="background:${color}"></span>${escapeHtml(item.name)}</span>`;
    })
    .join("");

  legend.innerHTML = `${baseHtml}${portHtml}`;
}

function updateStats() {
  const nodeTotal = state.graphView.nodes.size;
  const linkTotal = state.graphView.links.size;
  const packetTotal = state.filteredPackets.length;
  graphStats.textContent = `${nodeTotal} nodes - ${linkTotal} links`;
  packetCount.textContent = `${packetTotal} packets`;
  graphTime.textContent = state.lastUpdate ? `Last update ${formatTime(state.lastUpdate)}` : "Last update -";
}

function updatePacketRowSelection(prevId, nextId) {
  if (!packetRows) return;
  if (prevId !== null && prevId !== undefined) {
    const prevRow = packetRows.querySelector(`tr[data-id="${prevId}"]`);
    if (prevRow) {
      prevRow.classList.remove("active");
    }
  }
  if (nextId !== null && nextId !== undefined) {
    const nextRow = packetRows.querySelector(`tr[data-id="${nextId}"]`);
    if (nextRow) {
      nextRow.classList.add("active");
    }
  }
}

function setSelected(packet) {
  const prevId = state.selectedId;
  state.selectedId = packet.id;
  selectedMeta.textContent = `#${packet.id} - ${packet.portname || "Unknown"}`;
  detailType.textContent = packet.portname || "Unknown";
  detailType.classList.remove("pill-muted");
  packetDetails.textContent = formatPacketDetails(packet);
  updatePacketRowSelection(prevId, state.selectedId);
}

function clearSelection() {
  const prevId = state.selectedId;
  state.selectedId = null;
  selectedMeta.textContent = "Select a packet to inspect";
  detailType.textContent = "--";
  detailType.classList.add("pill-muted");
  packetDetails.textContent = "Waiting for packets...";
  updatePacketRowSelection(prevId, null);
}

function formatPacketDetails(packet) {
  const details =
    packet.details && typeof packet.details === "object" ? packet.details : {};
  const fromLabel = packet.from_label || nodeLabel(packet.from_id);
  const toLabel = packet.to_label || nodeLabel(packet.to_id);
  const lines = [
    `Type: ${packet.portname || "Unknown"}`,
    `From: ${fromLabel}`,
    `To: ${toLabel}`,
    `Time: ${formatTime(packet.created_at)}`,
    `Channel: ${packet.channel ?? "--"}`,
    `Gateway: ${packet.gateway_id || "--"}`,
    `Hops: ${formatHops(packet)}`,
    `RSSI: ${packet.rssi ?? "--"} dBm`,
    `SNR: ${packet.snr ?? "--"} dB`,
    "",
    "Decode:",
  ];
  const detailText =
    details && Object.keys(details).length ? JSON.stringify(details, null, 2) : "--";
  lines.push(detailText);
  lines.push("", "Text:");
  lines.push(packet.text || (details && details.text) || "--");
  lines.push("", "Payload (base64):");
  lines.push(packet.payload_b64 || "--");
  return lines.join("\n");
}

function extractChatText(packet) {
  if (!packet) return null;
  const portname = (packet.portname || "").toUpperCase();
  if (!portname.includes("TEXT_MESSAGE")) {
    return null;
  }
  const text = packet.text || (packet.details && packet.details.text);
  if (!text) return null;
  const trimmed = String(text).replace(/\s+/g, " ").trim();
  if (!trimmed) return null;
  if (trimmed.length <= TEXT_BUBBLE_MAX_CHARS) {
    return trimmed;
  }
  return `${trimmed.slice(0, TEXT_BUBBLE_MAX_CHARS - 3)}...`;
}

function buildPacketRow(packet) {
  const activeClass = packet.id === state.selectedId ? "active" : "";
  const signal = formatSignal(packet);
  const color = colorForPort(packet.portnum);
  const fromLabel = packet.from_label || nodeLabel(packet.from_id);
  const toLabel = packet.to_label || nodeLabel(packet.to_id);
  const rowStyle = `style="--row-accent:${color}"`;
  return `
    <tr data-id="${packet.id}" class="${activeClass}" ${rowStyle}>
      <td>${escapeHtml(formatTime(packet.created_at))}</td>
      <td>${escapeHtml(fromLabel)}</td>
      <td>${escapeHtml(toLabel)}</td>
      <td><span style="color:${color}">${escapeHtml(packet.portname || "Unknown")}</span></td>
      <td>${escapeHtml(payloadSize(packet))} B</td>
      <td>${escapeHtml(signal)}</td>
      <td>${escapeHtml(packet.channel ?? "--")}</td>
      <td>${escapeHtml(formatHops(packet))}</td>
      <td>${escapeHtml(packet.gateway_id || "--")}</td>
      <td>${escapeHtml(previewText(packet))}</td>
    </tr>
  `;
}

function renderPackets(options = {}) {
  const query = state.filters.search.trim().toLowerCase();
  const shouldFilter = query.length > 0;
  const filtered = shouldFilter
    ? state.packets.filter((packet) => packetMatchesSearch(packet, query))
    : state.packets.slice();
  state.filteredPackets = filtered;

  const shouldIncremental =
    options.incremental === true &&
    !shouldFilter &&
    packetRenderState.query === query &&
    packetRenderState.ids.length > 0;

  if (!packetRows) {
    updateStats();
    return;
  }

  if (!shouldIncremental) {
    packetRows.innerHTML = filtered.map((packet) => buildPacketRow(packet)).join("");
    packetRenderState.ids = filtered.map((packet) => packet.id);
    packetRenderState.query = query;
    if (packetTableWrap) {
      packetTableWrap.scrollTop = 0;
    }
  } else {
    const anchorId = packetRenderState.ids[0];
    const anchorIndex = state.packets.findIndex((packet) => packet.id === anchorId);
    if (anchorIndex === -1) {
      packetRows.innerHTML = filtered.map((packet) => buildPacketRow(packet)).join("");
      packetRenderState.ids = filtered.map((packet) => packet.id);
    } else if (anchorIndex > 0) {
      const newPackets = state.packets.slice(0, anchorIndex);
      const follow = packetTableWrap ? packetTableWrap.scrollTop <= FEED_FOLLOW_THRESHOLD_PX : true;
      const prevHeight = packetTableWrap ? packetTableWrap.scrollHeight : 0;
      packetRows.insertAdjacentHTML(
        "afterbegin",
        newPackets.map((packet) => buildPacketRow(packet)).join(""),
      );
      packetRenderState.ids = newPackets.map((packet) => packet.id).concat(packetRenderState.ids);
      const maxRows = filtered.length;
      if (packetRenderState.ids.length > maxRows) {
        const overflow = packetRenderState.ids.length - maxRows;
        for (let i = 0; i < overflow; i += 1) {
          const last = packetRows.lastElementChild;
          if (!last) break;
          last.remove();
        }
        packetRenderState.ids = packetRenderState.ids.slice(0, maxRows);
      }
      if (packetTableWrap) {
        const nextHeight = packetTableWrap.scrollHeight;
        if (follow) {
          packetTableWrap.scrollTop = 0;
        } else {
          packetTableWrap.scrollTop += nextHeight - prevHeight;
        }
      }
    }
    packetRenderState.query = query;
  }

  if (!filtered.length) {
    clearSelection();
  } else if (!filtered.some((packet) => packet.id === state.selectedId)) {
    setSelected(filtered[0]);
  } else {
    updatePacketRowSelection(null, state.selectedId);
  }

  updateStats();
}

function enqueuePacket(packet) {
  packetQueue.push(packet);
  if (!packetFlushPending) {
    packetFlushPending = true;
    requestAnimationFrame(flushPacketQueue);
  }
}

function flushPacketQueue() {
  packetFlushPending = false;
  let graphChanged = false;
  let hasRoutes = false;
  let processed = 0;
  const start = performance.now();
  while (
    packetQueue.length &&
    processed < MAX_PACKETS_PER_FLUSH &&
    performance.now() - start < PACKET_FLUSH_BUDGET_MS
  ) {
    const packet = packetQueue.shift();
    const result = addPacket(packet, { deferRender: true });
    if (result) {
      graphChanged = graphChanged || result.graphChanged;
      hasRoutes = hasRoutes || result.hasRoutes;
    }
    processed += 1;
  }
  if (processed) {
    renderPackets({ incremental: true });
    if (graphChanged) {
      updateGraphView({ reheat: true });
    } else if (hasRoutes) {
      updateGraphView({ reheat: false });
    } else {
      const now = performance.now();
      if (now - lastStatsUpdate >= STATS_UPDATE_INTERVAL_MS) {
        updateLegend(state.graphView.links);
        updateStats();
        lastStatsUpdate = now;
      }
    }
  }
  if (packetQueue.length) {
    packetFlushPending = true;
    requestAnimationFrame(flushPacketQueue);
  }
}

function renderNodes() {
  const query = state.filters.search.trim().toLowerCase();
  const filteredNodes = state.nodesSummary.filter((node) => nodeMatchesSearch(node, query));
  const rows = filteredNodes.map((node) => {
      const label = nodeLabelFromInfo(node.node_id, node);
      const lastSeen = node.last_seen ? formatTime(node.last_seen) : "--";
      const rssi = node.avg_rssi !== null && node.avg_rssi !== undefined ? `${Math.round(node.avg_rssi)} dBm` : "--";
      return `
        <tr data-id="${node.node_id}">
          <td>${escapeHtml(label)}</td>
          <td>${escapeHtml(lastSeen)}</td>
          <td>${escapeHtml(node.packet_count ?? 0)}</td>
          <td>${escapeHtml(rssi)}</td>
        </tr>
      `;
    })
    .join("");

  nodeRows.innerHTML = rows;
  nodeCount.textContent = `${filteredNodes.length} nodes`;
}

function updateGraphView(options = {}) {
  const portFilterValue = state.filters.portnum !== "" ? Number(state.filters.portnum) : null;
  const hasPortFilter = portFilterValue !== null && !Number.isNaN(portFilterValue);
  const viewLinks = new Map();
  state.links.forEach((link, key) => {
    if (hasPortFilter && link.portnum !== portFilterValue) {
      return;
    }
    viewLinks.set(key, link);
  });

  const viewNodes = new Map();
  if (!hasPortFilter) {
    state.nodes.forEach((node, nodeId) => {
      viewNodes.set(nodeId, node);
    });
  } else if (viewLinks.size) {
    viewLinks.forEach((link) => {
      const sourceId = link.sourceId ?? link.source;
      const targetId = link.targetId ?? link.target;
      if (state.nodes.has(sourceId)) {
        viewNodes.set(sourceId, state.nodes.get(sourceId));
      }
      if (state.nodes.has(targetId)) {
        viewNodes.set(targetId, state.nodes.get(targetId));
      }
    });
  }

  const prevNodes = state.graphView.nodes.size;
  const prevLinks = state.graphView.links.size;
  state.graphView = { nodes: viewNodes, links: viewLinks };
  const sizeChanged = viewNodes.size !== prevNodes || viewLinks.size !== prevLinks;
  graph.update(viewNodes, viewLinks, {
    reheat: options.reheat || sizeChanged,
  });
  updateLegend(viewLinks);
  updateStats();
  refreshSelectedFocus();
}

function renderMetrics(metrics) {
  metricPpm.textContent = metrics.packets_per_min ?? "--";
  metricActive.textContent = metrics.active_nodes ?? "--";
  if (metrics.top_ports && metrics.top_ports.length) {
    const top = metrics.top_ports[0];
    metricPort.textContent = `${top.portname || top.portnum} (${top.count})`;
  } else {
    metricPort.textContent = "--";
  }
  metricRssi.textContent = metrics.median_rssi !== null && metrics.median_rssi !== undefined ? `${Math.round(metrics.median_rssi)} dBm` : "--";
  metricSnr.textContent = metrics.median_snr !== null && metrics.median_snr !== undefined ? `${Math.round(metrics.median_snr)} dB` : "--";
}

function applyGraphData(graphData) {
  const existingNodes = new Map(state.nodes);
  (graphData.nodes || []).forEach((node) => {
    const current = existingNodes.get(node.id);
    if (current) {
      current.label = node.label || current.label;
      current.isBroadcast = node.id === BROADCAST_ID;
      if (!current.color) {
        current.color = colorForNode(node.id);
      }
      if (current.heat === undefined) {
        current.heat = 0;
        current.lastHeatAt = 0;
      }
      if (current.lastSendColor === undefined) {
        current.lastSendColor = null;
        current.lastReceiveColor = null;
      }
    } else {
      existingNodes.set(node.id, {
        id: node.id,
        label: node.label || formatNodeId(node.id),
        count: 0,
        lastActiveAt: 0,
        lastSeenEpoch: null,
        isBroadcast: node.id === BROADCAST_ID,
        color: colorForNode(node.id),
        heat: 0,
        lastHeatAt: 0,
        lastSendColor: null,
        lastReceiveColor: null,
      });
    }
  });
  state.nodes = existingNodes;

  const newLinks = new Map();
  (graphData.links || []).forEach((link) => {
    const key = `${link.source}-${link.target}-${link.portnum}`;
    const existing = state.links.get(key);
    const apiLastSeen = link.last_seen || 0;
    const lastSeen = Math.max(existing ? existing.lastSeen || 0 : 0, apiLastSeen);
    const heat = existing ? existing.heat || 0 : 0;
    const lastHeatAt = existing ? existing.lastHeatAt || 0 : 0;
    newLinks.set(key, {
      ...link,
      sourceId: link.source,
      targetId: link.target,
      count: link.count || 0,
      lastSeen,
      heat,
      lastHeatAt,
      flashUntil: existing ? existing.flashUntil : 0,
    });
  });
  state.links = newLinks;
  updateLoadingState();
}

function applyNodesSummary(nodes) {
  state.nodesSummary = nodes;
  const nameMap = new Map();
  nodes.forEach((node) => {
    nameMap.set(node.node_id, nodeLabelFromInfo(node.node_id, node));
    if (!state.nodes.has(node.node_id)) {
      state.nodes.set(node.node_id, {
        id: node.node_id,
        label: nodeLabelFromInfo(node.node_id, node),
        count: node.packet_count || 0,
        lastActiveAt: 0,
        lastSeenEpoch: node.last_seen || null,
        color: colorForNode(node.node_id),
        heat: 0,
        lastHeatAt: 0,
        lastSendColor: null,
        lastReceiveColor: null,
      });
    } else {
      const graphNode = state.nodes.get(node.node_id);
      graphNode.label = nodeLabelFromInfo(node.node_id, node);
      graphNode.count = node.packet_count || graphNode.count || 0;
      graphNode.lastSeenEpoch = node.last_seen || graphNode.lastSeenEpoch;
      if (!graphNode.color) {
        graphNode.color = colorForNode(node.node_id);
      }
      if (graphNode.heat === undefined) {
        graphNode.heat = 0;
        graphNode.lastHeatAt = 0;
      }
      if (graphNode.lastSendColor === undefined) {
        graphNode.lastSendColor = null;
        graphNode.lastReceiveColor = null;
      }
    }
  });
  state.nodeNames = nameMap;
  updateLoadingState();
}

function applyPacketsData(packets) {
  state.packets = packets.map((packet) => normalizePacket(packet));
  state.lastUpdate = state.packets.length ? state.packets[0].created_at : null;
  updateLoadingState();
}

function ensureNode(nodeId, label, options = {}) {
  if (nodeId === null || nodeId === undefined) {
    return null;
  }
  const allowBroadcast = options.allowBroadcast === true;
  if (nodeId === BROADCAST_ID && !allowBroadcast) {
    return null;
  }
  if (!state.nodes.has(nodeId)) {
    state.nodes.set(nodeId, {
      id: nodeId,
      label: label || nodeLabel(nodeId),
      count: 0,
      lastActiveAt: 0,
      lastSeenEpoch: null,
      isBroadcast: nodeId === BROADCAST_ID,
      color: colorForNode(nodeId),
      heat: 0,
      lastHeatAt: 0,
      lastSendColor: null,
      lastReceiveColor: null,
    });
  }
  const node = state.nodes.get(nodeId);
  if (!node.color) {
    node.color = colorForNode(nodeId);
  }
  if (node.heat === undefined) {
    node.heat = 0;
    node.lastHeatAt = 0;
  }
  if (node.lastSendColor === undefined) {
    node.lastSendColor = null;
    node.lastReceiveColor = null;
  }
  if (nodeId === BROADCAST_ID) {
    node.label = "broadcast";
    node.isBroadcast = true;
  } else if (label && node.label !== label) {
    node.label = label;
  }
  return node;
}

function bumpNodeHeat(node, amount, now) {
  if (!node || !Number.isFinite(amount)) {
    return;
  }
  const age = node.lastHeatAt ? now - node.lastHeatAt : 0;
  const cooled = node.heat ? node.heat * Math.exp(-age / NODE_HEAT_HALF_LIFE_MS) : 0;
  node.heat = Math.min(NODE_HEAT_MAX, cooled + amount);
  node.lastHeatAt = now;
}

function coerceNodeId(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

function buildRoutePath(routeList, fromId, toId) {
  const path = [];
  if (fromId !== null && fromId !== undefined) {
    path.push(fromId);
  }
  if (Array.isArray(routeList)) {
    routeList.forEach((value) => {
      const nodeId = coerceNodeId(value);
      if (nodeId !== null) {
        path.push(nodeId);
      }
    });
  }
  if (toId !== null && toId !== undefined) {
    path.push(toId);
  }
  return path.filter((nodeId, idx) => idx === 0 || nodeId !== path[idx - 1]);
}

function extractRoutePaths(packet) {
  const details = packet.details && typeof packet.details === "object" ? packet.details : {};
  const paths = [];
  const pushPath = (routeList, fromId, toId, direction) => {
    if (!Array.isArray(routeList)) return;
    const path = buildRoutePath(routeList, fromId, toId);
    if (path.length > 1) {
      paths.push({ path, direction });
    }
  };
  if (packet.portnum === TRACEROUTE_PORTNUM) {
    pushPath(details.route, packet.from_id, packet.to_id, "forward");
    pushPath(details.route_back, packet.to_id, packet.from_id, "return");
  }
  if (packet.portnum === ROUTING_PORTNUM) {
    if (details.route_request) {
      pushPath(details.route_request.route, packet.from_id, packet.to_id, "forward");
    }
    if (details.route_reply) {
      pushPath(details.route_reply.route, packet.to_id, packet.from_id, "return");
    }
  }
  return paths;
}

function animateRouteForPacket(packet) {
  const paths = extractRoutePaths(packet);
  if (!paths.length) {
    return { addedNode: false, hasRoutes: false };
  }
  let addedNode = false;
  paths.forEach((entry) => {
    const direction = entry.direction || "forward";
    const path = entry.path || [];
    const color = direction === "return" ? ROUTE_COLOR_RETURN : ROUTE_COLOR_FORWARD;
    path.forEach((nodeId) => {
      const hadNode = state.nodes.has(nodeId);
      const node = ensureNode(nodeId, nodeLabel(nodeId), { allowBroadcast: true });
      if (!hadNode && node) {
        addedNode = true;
        node.lastSeenEpoch = packet.created_at || node.lastSeenEpoch;
      }
    });
    graph.animateRoute(path, packet.portnum, {
      stepMs: ROUTE_STEP_MS,
      fadeMs: ROUTE_FADE_MS,
      color,
      direction,
    });
  });
  return { addedNode, hasRoutes: true };
}

function addPacket(packet, options = {}) {
  if (state.paused) return { graphChanged: false, hasRoutes: false };
  const deferRender = options.deferRender === true;
  const normalized = normalizePacket(packet);
  if (!packetMatchesFilters(normalized)) {
    return { graphChanged: false, hasRoutes: false };
  }
  state.packets.unshift(normalized);
  state.packets = state.packets.slice(0, PACKET_RENDER_LIMIT);

  const source = normalized.from_id;
  const target = normalized.to_id;
  const portnum = normalized.portnum ?? null;
  const portname = normalized.portname || "UNKNOWN";
  const isBroadcast = target === BROADCAST_ID;
  const now = performance.now();
  let graphChanged = false;
  const portColor = colorForPort(portnum);
  const chatText = extractChatText(normalized);
  if (chatText && source !== null && source !== undefined) {
    graph.addMessageBubble(source, chatText, { color: portColor });
  }
  graph.bumpActivity(isBroadcast ? 0.6 : 1);

  const hadSource = source !== null && source !== undefined && state.nodes.has(source);
  const sourceNode = ensureNode(source, normalized.from_label);
  if (sourceNode) {
    sourceNode.count += 1;
    sourceNode.lastActiveAt = now;
    sourceNode.lastSeenEpoch = normalized.created_at;
    sourceNode.lastSentAt = now;
    sourceNode.lastSendColor = portColor;
    bumpNodeHeat(sourceNode, isBroadcast ? NODE_HEAT_GAIN * 0.55 : NODE_HEAT_GAIN, now);
  }
  if (sourceNode && !hadSource) {
    graphChanged = true;
  }

  if (!isBroadcast) {
    const hadTarget = target !== null && target !== undefined && state.nodes.has(target);
    const targetNode = ensureNode(target, normalized.to_label);
    if (targetNode) {
      targetNode.count += 1;
      targetNode.lastActiveAt = now;
      targetNode.lastSeenEpoch = normalized.created_at;
      targetNode.lastReceivedAt = now;
      targetNode.lastReceiveColor = portColor;
      bumpNodeHeat(targetNode, NODE_HEAT_GAIN * 0.9, now);
    }
    if (targetNode && !hadTarget) {
      graphChanged = true;
    }
  } else {
    const broadcastNode = ensureNode(BROADCAST_ID, "broadcast", { allowBroadcast: true });
    if (broadcastNode) {
      broadcastNode.count += 1;
      broadcastNode.lastActiveAt = now;
      broadcastNode.lastSeenEpoch = normalized.created_at;
      bumpNodeHeat(broadcastNode, NODE_HEAT_GAIN * 0.4, now);
    }
  }

  if (source !== null && source !== undefined) {
    if (isBroadcast) {
      graph.pulse(source, portnum, { kind: "broadcast" });
    } else {
      graph.pulse(source, portnum, { kind: "send" });
    }
  }
  if (isBroadcast) {
    graph.pulse(BROADCAST_ID, portnum, { kind: "broadcast-core" });
  } else if (target !== null && target !== undefined) {
    graph.pulse(target, portnum, { kind: "receive" });
  }

  if (source !== null && target !== null && source !== undefined && target !== undefined) {
    const key = `${source}-${target}-${portnum}`;
    if (!state.links.has(key)) {
      graphChanged = true;
      state.links.set(key, {
        source,
        target,
        sourceId: source,
        targetId: target,
        portnum,
        portname,
        count: 0,
        lastSeen: 0,
        heat: 0,
        lastHeatAt: 0,
        flashUntil: 0,
      });
    }
    const link = state.links.get(key);
    link.count += 1;
    link.lastSeen = normalized.created_at || Math.floor(Date.now() / 1000);
    link.flashUntil = now + LINK_FLASH_MS;
    const heatAge = link.lastHeatAt ? now - link.lastHeatAt : 0;
    const cooledHeat = link.heat ? link.heat * Math.exp(-heatAge / LINK_HEAT_HALF_LIFE_MS) : 0;
    link.heat = Math.min(LINK_HEAT_MAX, cooledHeat + LINK_HEAT_GAIN);
    link.lastHeatAt = now;
    if (!isBroadcast) {
      graph.linkShockwave(source, target, portnum);
    }
  }

  const routeResult = animateRouteForPacket(normalized);
  if (routeResult.addedNode) {
    graphChanged = true;
  }

  state.lastUpdate = normalized.created_at || Math.floor(Date.now() / 1000);
  if (deferRender) {
    return { graphChanged, hasRoutes: routeResult.hasRoutes };
  }

  renderPackets({ incremental: true });
  if (graphChanged) {
    updateGraphView({ reheat: true });
    return { graphChanged, hasRoutes: routeResult.hasRoutes };
  }
  if (routeResult.hasRoutes) {
    updateGraphView({ reheat: false });
    return { graphChanged, hasRoutes: routeResult.hasRoutes };
  }
  updateLegend(state.graphView.links);
  updateStats();
  return { graphChanged, hasRoutes: routeResult.hasRoutes };
}

function fillDrawerContent(container, lines) {
  container.innerHTML = "";
  lines.forEach((line) => {
    const div = document.createElement("div");
    div.textContent = line;
    container.appendChild(div);
  });
}

function setDrawerOpen(open) {
  nodeDrawer.classList.toggle("open", open);
  document.body.classList.toggle("drawer-open", open);
}

function collectSelectedLinks(nodeId, linksSource = state.links) {
  const links = [];
  linksSource.forEach((link) => {
    const sourceId =
      link.sourceId ?? (typeof link.source === "object" ? link.source.id : link.source);
    const targetId =
      link.targetId ?? (typeof link.target === "object" ? link.target.id : link.target);
    if (sourceId === null || targetId === null || sourceId === undefined || targetId === undefined) {
      return;
    }
    if (sourceId !== nodeId && targetId !== nodeId) {
      return;
    }
    links.push({
      sourceId,
      targetId,
      portnum: link.portnum ?? null,
      count: link.count || 0,
      lastSeen: link.lastSeen || 0,
    });
  });
  return links;
}

function buildNodeHistoryQuery(options = {}) {
  const params = new URLSearchParams();
  if (state.filters.window) {
    params.set("window", String(state.filters.window));
  }
  if (state.filters.channel !== "") {
    params.set("channel", state.filters.channel);
  }
  if (state.filters.gateway) {
    params.set("gateway", state.filters.gateway);
  }
  if (options.nodeId !== null && options.nodeId !== undefined) {
    params.set("node", String(options.nodeId));
  }
  if (options.portnum) {
    params.set("portnum", options.portnum);
  }
  if (options.limit) {
    params.set("limit", String(options.limit));
  }
  return params.toString();
}

function collectRouteHistory(packets = []) {
  const routes = [];
  const seen = new Set();
  packets.forEach((packet) => {
    const normalized = normalizePacket(packet);
    const entries = extractRoutePaths(normalized);
    entries.forEach((entry) => {
      const path = entry.path || [];
      if (path.length < 2) return;
      const direction = entry.direction || "forward";
      const key = `${direction}:${path.join("-")}`;
      if (seen.has(key)) return;
      seen.add(key);
      routes.push({
        path,
        direction,
        color: direction === "return" ? ROUTE_COLOR_RETURN : ROUTE_COLOR_FORWARD,
      });
    });
  });
  return routes;
}

function refreshSelectedFocus() {
  if (state.selectedNodeId === null || state.selectedNodeId === undefined) {
    return;
  }
  const links = state.selectedLinks.length
    ? state.selectedLinks
    : collectSelectedLinks(state.selectedNodeId);
  graph.setSelectedFocus({
    nodeId: state.selectedNodeId,
    links,
    routes: state.selectedRoutes,
  });
}

async function selectNode(nodeId) {
  if (nodeId === null || nodeId === undefined) return;
  if (nodeId === BROADCAST_ID) return;
  state.selectedNodeId = nodeId;
  state.selectionRequestId += 1;
  const requestId = state.selectionRequestId;
  const links = collectSelectedLinks(nodeId);
  state.selectedLinks = links;
  state.selectedRoutes = [];
  graph.setSelectedFocus({ nodeId, links: state.selectedLinks, routes: [] });
  openNodeDrawer(nodeId);
  if (!state.paused) {
    setPaused(true, { auto: true });
  }

  const graphData = await fetchJson(`/api/graph?${buildFilterQuery({ excludePort: true })}`);
  if (requestId !== state.selectionRequestId || state.selectedNodeId !== nodeId) {
    return;
  }
  if (graphData && Array.isArray(graphData.links)) {
    const fullLinks = collectSelectedLinks(nodeId, graphData.links);
    if (fullLinks.length) {
      state.selectedLinks = fullLinks;
    }
  }
  refreshSelectedFocus();

  const portnum = `${TRACEROUTE_PORTNUM},${ROUTING_PORTNUM}`;
  const packets = await fetchJson(
    `/api/packets?${buildNodeHistoryQuery({ nodeId, portnum, limit: 1000 })}`,
  );
  if (requestId !== state.selectionRequestId || state.selectedNodeId !== nodeId) {
    return;
  }
  const routes = collectRouteHistory(packets || []);
  state.selectedRoutes = routes;
  let addedNode = false;
  state.selectedLinks.forEach((link) => {
    [link.sourceId, link.targetId].forEach((linkNodeId) => {
      const hadNode = state.nodes.has(linkNodeId);
      const node = ensureNode(linkNodeId, nodeLabel(linkNodeId), { allowBroadcast: true });
      if (!hadNode && node) {
        addedNode = true;
      }
    });
  });
  routes.forEach((route) => {
    route.path.forEach((routeNodeId) => {
      const hadNode = state.nodes.has(routeNodeId);
      const node = ensureNode(routeNodeId, nodeLabel(routeNodeId), { allowBroadcast: true });
      if (!hadNode && node) {
        addedNode = true;
      }
    });
  });
  if (addedNode) {
    updateGraphView({ reheat: true });
  }
  refreshSelectedFocus();
}
async function openNodeDrawer(nodeId) {
  if (!nodeId && nodeId !== 0) return;
  state.activeNodeId = nodeId;
  const requestId = state.drawerRequestId + 1;
  state.drawerRequestId = requestId;

  setDrawerOpen(true);
  drawerTitle.textContent = nodeLabel(nodeId);
  drawerSubtitle.textContent = formatNodeId(nodeId);

  const data = await fetchJson(`/api/node/${nodeId}?${buildFilterQuery({ limit: 30 })}`);
  if (!data || requestId !== state.drawerRequestId) {
    return;
  }

  const node = data.node || {};
  fillDrawerContent(drawerIdentity, [
    `Short: ${node.short_name || "--"}`,
    `Long: ${node.long_name || "--"}`,
    `Node Id: ${formatNodeId(node.node_id ?? nodeId)}`,
    `Last Seen: ${node.last_seen ? formatTime(node.last_seen) : "--"}`,
  ]);

  const ports = (data.ports || []).slice(0, 6).map((port) => {
    const label = port.portname || port.portnum;
    const lastSeen = port.last_seen ? formatTime(port.last_seen) : "--";
    return `${label} (${port.count}) - ${lastSeen}`;
  });
  fillDrawerContent(drawerPorts, ports.length ? ports : ["--"]);

  const peers = (data.peers || []).slice(0, 6).map((peer) => {
    const label = nodeLabel(peer.peer_id);
    const lastSeen = peer.last_seen ? formatTime(peer.last_seen) : "--";
    return `${label} (${peer.count}) - ${lastSeen}`;
  });
  fillDrawerContent(drawerPeers, peers.length ? peers : ["--"]);

  const packets = (data.packets || []).slice(0, 6).map((packet) => {
    const fromLabel = packet.from_label || nodeLabel(packet.from_id);
    const toLabel = packet.to_label || nodeLabel(packet.to_id);
    return `${formatTime(packet.created_at)} - ${fromLabel} -> ${toLabel} (${packet.portname || "Unknown"})`;
  });
  fillDrawerContent(drawerPackets, packets.length ? packets : ["--"]);
}

function clearNodeSelection(options = {}) {
  const keepDrawer = options.keepDrawer === true;
  if (state.selectedNodeId !== null && state.selectedNodeId !== undefined) {
    state.selectedNodeId = null;
    state.selectedRoutes = [];
    state.selectedLinks = [];
    graph.setSelectedFocus(null);
    if (state.autoPaused) {
      setPaused(false, { auto: true });
    }
  }
  if (!keepDrawer) {
    state.activeNodeId = null;
    setDrawerOpen(false);
  }
}

function closeNodeDrawer() {
  clearNodeSelection({ keepDrawer: false });
}

async function fetchJson(path, options = {}) {
  const timeoutMs = Number.isFinite(options.timeoutMs) ? options.timeoutMs : FETCH_TIMEOUT_MS;
  const hasAbort = typeof AbortController !== "undefined";
  const controller = hasAbort ? new AbortController() : null;
  const timeoutId = hasAbort && timeoutMs > 0
    ? setTimeout(() => controller.abort(), timeoutMs)
    : null;

  try {
    const response = await fetch(path, {
      cache: "no-store",
      signal: controller ? controller.signal : undefined,
    });
    if (!response.ok) {
      return null;
    }
    return await response.json();
  } catch (error) {
    return null;
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function getStressConfig(params) {
  if (!params) return null;
  const rate = Number(params.get("stress"));
  if (!Number.isFinite(rate) || rate <= 0) {
    return null;
  }
  const durationSeconds = Number(params.get("stressDur"));
  const durationMs = Number.isFinite(durationSeconds) ? durationSeconds * 1000 : 15000;
  const nodes = Math.max(20, Math.min(800, Number(params.get("stressNodes")) || 220));
  const ports = Math.max(3, Math.min(16, Number(params.get("stressPorts")) || 8));
  const channels = Math.max(1, Math.min(8, Number(params.get("stressChannels")) || 3));
  return { rate, durationMs, nodes, ports, channels };
}

function initPerfStats(params, stressConfig) {
  const enabled = params.has("profile") || Boolean(stressConfig);
  const stats = {
    enabled,
    fps: 0,
    frames: 0,
    lastFpsAt: performance.now(),
    drawMsTotal: 0,
    drawMsSamples: 0,
    avgDrawMs: 0,
    maxDrawMs: 0,
  };
  if (enabled) {
    window.meshvizPerf = stats;
  }
  return stats;
}

function recordPerfFrame(stats, now, drawMs) {
  if (!stats || !stats.enabled) return;
  stats.drawMsTotal += drawMs;
  stats.drawMsSamples += 1;
  stats.avgDrawMs = stats.drawMsTotal / stats.drawMsSamples;
  stats.maxDrawMs = Math.max(stats.maxDrawMs, drawMs);
  stats.frames += 1;
  const elapsed = now - stats.lastFpsAt;
  if (elapsed >= 1000) {
    stats.fps = Math.round((stats.frames * 1000) / elapsed);
    stats.frames = 0;
    stats.lastFpsAt = now;
  }
}

function startStressTest(config) {
  if (!config) return;
  const baseId = 1_000_000_000;
  let nextId = baseId;
  const nowEpoch = Math.floor(Date.now() / 1000);
  const nodeIds = Array.from({ length: config.nodes }, (_, idx) => 0x100000 + idx);
  const portnums = Array.from({ length: config.ports }, (_, idx) => idx + 1);
  const channels = Array.from({ length: config.channels }, (_, idx) => idx);
  const tickMs = 100;
  const packetsPerTick = Math.max(1, Math.round((config.rate * tickMs) / 1000));
  const stats = {
    rate: config.rate,
    durationMs: config.durationMs,
    sent: 0,
    startedAt: performance.now(),
    finishedAt: null,
  };
  window.meshvizStress = stats;
  let perfTimer = null;
  if (perfStats && perfStats.enabled) {
    perfTimer = window.setInterval(() => {
      console.log(
        `[perf] fps=${perfStats.fps} avgDraw=${perfStats.avgDrawMs.toFixed(1)}ms maxDraw=${perfStats.maxDrawMs.toFixed(1)}ms`,
      );
    }, 2000);
  }
  const timer = window.setInterval(() => {
    const now = performance.now();
    const epoch = nowEpoch + Math.floor((now - stats.startedAt) / 1000);
    for (let i = 0; i < packetsPerTick; i += 1) {
      const fromIndex = Math.floor(Math.random() * nodeIds.length);
      let toIndex = Math.floor(Math.random() * nodeIds.length);
      if (toIndex === fromIndex) {
        toIndex = (toIndex + 1) % nodeIds.length;
      }
      const portnum = portnums[Math.floor(Math.random() * portnums.length)];
      const channel = channels[Math.floor(Math.random() * channels.length)];
      enqueuePacket({
        id: nextId++,
        created_at: epoch,
        from_id: nodeIds[fromIndex],
        to_id: nodeIds[toIndex],
        portnum,
        portname: `Port ${portnum}`,
        channel,
        gateway_id: `gw-${(fromIndex % 5) + 1}`,
        text: `Synthetic packet ${stats.sent + i + 1}`,
        payload_b64: "c3RyZXNz",
      });
    }
    stats.sent += packetsPerTick;
    if (now - stats.startedAt >= config.durationMs) {
      window.clearInterval(timer);
      if (perfTimer) {
        window.clearInterval(perfTimer);
      }
      stats.finishedAt = performance.now();
      const elapsedSec = (stats.finishedAt - stats.startedAt) / 1000;
      stats.actualRate = Math.round(stats.sent / Math.max(1, elapsedSec));
      console.log(
        `[stress] sent ${stats.sent} packets in ${elapsedSec.toFixed(1)}s (~${stats.actualRate}/s).`,
      );
    }
  }, tickMs);
}

function buildFilterQuery(options = {}) {
  const params = new URLSearchParams();
  if (state.filters.window) {
    params.set("window", String(state.filters.window));
  }
  if (!options.excludePort && state.filters.portnum !== "") {
    params.set("portnum", state.filters.portnum);
  }
  if (!options.excludeChannel && state.filters.channel !== "") {
    params.set("channel", state.filters.channel);
  }
  if (state.filters.gateway) {
    params.set("gateway", state.filters.gateway);
  }
  if (options.limit) {
    params.set("limit", String(options.limit));
  }
  return params.toString();
}

async function refreshPackets() {
  const packets = await fetchJson(`/api/packets?${buildFilterQuery({ limit: 200 })}`);
  if (!packets) {
    return;
  }
  applyPacketsData(packets);
  renderPackets();
}

async function refreshAll() {
  const baseQuery = buildFilterQuery();
  const graphPromise = fetchJson(`/api/graph?${baseQuery}`);
  const nodesPromise = fetchJson(`/api/nodes?${baseQuery}`);
  const metricsPromise = fetchJson(`/api/metrics?${baseQuery}`);
  const portsPromise = fetchJson(`/api/ports?${buildFilterQuery({ excludePort: true })}`);
  const channelsPromise = fetchJson(`/api/channels?${buildFilterQuery({ excludeChannel: true })}`);
  const packetsPromise = fetchJson(`/api/packets?${buildFilterQuery({ limit: 200 })}`);

  const [
    graphData,
    nodesData,
    metricsData,
    portsData,
    channelsData,
    packetsData,
  ] = await Promise.all([
    graphPromise,
    nodesPromise,
    metricsPromise,
    portsPromise,
    channelsPromise,
    packetsPromise,
  ]);

  if (graphData) {
    applyGraphData(graphData);
  }
  if (nodesData) {
    applyNodesSummary(nodesData);
  }
  if (metricsData) {
    renderMetrics(metricsData);
  }
  if (portsData) {
    populatePortFilter(portsData);
  }
  if (channelsData) {
    populateChannelFilter(channelsData);
  }
  if (packetsData) {
    applyPacketsData(packetsData);
  }

  renderNodes();
  renderPackets();
  updateGraphView({ reheat: true });
  updateLoadingState();
}

async function refreshSummary() {
  const baseQuery = buildFilterQuery();
  const graphPromise = fetchJson(`/api/graph?${baseQuery}`);
  const nodesPromise = fetchJson(`/api/nodes?${baseQuery}`);
  const metricsPromise = fetchJson(`/api/metrics?${baseQuery}`);
  const portsPromise = fetchJson(`/api/ports?${buildFilterQuery({ excludePort: true })}`);
  const channelsPromise = fetchJson(`/api/channels?${buildFilterQuery({ excludeChannel: true })}`);

  const [graphData, nodesData, metricsData, portsData, channelsData] = await Promise.all([
    graphPromise,
    nodesPromise,
    metricsPromise,
    portsPromise,
    channelsPromise,
  ]);

  if (graphData) {
    applyGraphData(graphData);
  }
  if (nodesData) {
    applyNodesSummary(nodesData);
  }
  if (metricsData) {
    renderMetrics(metricsData);
  }
  if (portsData) {
    populatePortFilter(portsData);
  }
  if (channelsData) {
    populateChannelFilter(channelsData);
  }

  renderNodes();
  updateGraphView({ reheat: false });
  updateLoadingState();
}

function populatePortFilter(ports) {
  const current = portFilter.value;
  const options = [
    "<option value=\"\">All</option>",
    ...ports
      .filter((port) => port.portnum !== null && port.portnum !== undefined)
      .map((port) => {
        const label = port.portname || `Port ${port.portnum}`;
        return `<option value=\"${port.portnum}\">${label} (${port.portnum})</option>`;
      }),
  ];
  portFilter.innerHTML = options.join("");
  if (current) {
    portFilter.value = current;
  }
}

function populateChannelFilter(channels) {
  const current = channelFilter.value;
  const options = [
    "<option value=\"\">All</option>",
    ...channels
      .filter((channel) => channel.channel !== null && channel.channel !== undefined)
      .map((channel) => `<option value=\"${channel.channel}\">${channel.channel}</option>`),
  ];
  channelFilter.innerHTML = options.join("");
  if (current) {
    channelFilter.value = current;
  }
}

function connectWs() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  if (state.socket) {
    state.socket.close();
  }
  state.connection = "connecting";
  updateLiveStatus();

  const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);
  state.socket = socket;

  socket.addEventListener("open", () => {
    state.connection = "live";
    updateLiveStatus();
  });

  socket.addEventListener("message", (event) => {
    if (state.paused) return;
    const packet = normalizePacket(JSON.parse(event.data));
    enqueuePacket(packet);
    updateLoadingState();
  });

  socket.addEventListener("close", () => {
    state.connection = "disconnected";
    updateLiveStatus();
    setTimeout(connectWs, 2000);
  });
}
const graph = (() => {
  const canvas = document.getElementById("graphCanvas");
  const ctx = canvas.getContext("2d");
  const pulses = [];
  const routeAnimations = [];
  const messageBubbles = new Map();
  const logoImage = new Image();
  logoImage.decoding = "async";
  logoImage.src = "logo.png";
  let selectedFocus = null;
  const SELECTION_FADE_MS = 1400;
  let activityEnergy = 0;
  let activitySpike = 0;
  let activityUpdatedAt = 0;
  let width = 0;
  let height = 0;
  let simulation = null;
  let nodes = [];
  let links = [];
  let nodeIndex = new Map();
  let transform = d3.zoomIdentity;
  let onNodeClick = null;
  let onNodeHover = null;
  let onBackgroundAction = null;
  let padding = 80;
  let lastNodeCount = 0;
  let lastLinkCount = 0;
  let aggregatedLinkGroups = [];
  let hoverIndex = null;
  let hoverIndexBuiltAt = 0;
  let hoverIndexNodeCount = 0;
  let homeForce = null;

  function resize() {
    const rect = canvas.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    width = rect.width;
    height = rect.height;
    canvas.width = width * ratio;
    canvas.height = height * ratio;
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    padding = Math.max(70, Math.min(width, height) * 0.08);
    const broadcastNode = nodeIndex.get(BROADCAST_ID);
    if (broadcastNode) {
      broadcastNode.fx = width / 2;
      broadcastNode.fy = height / 2;
    }
    applyDynamicForces();
  }

  function rebuildIndex() {
    nodeIndex = new Map(nodes.map((node) => [node.id, node]));
  }

  function rebuildAggregatedLinks() {
    const grouped = new Map();
    links.forEach((link) => {
      const sourceNode = resolveNode(link.source) || nodeIndex.get(link.sourceId);
      const targetNode = resolveNode(link.target) || nodeIndex.get(link.targetId);
      if (!sourceNode || !targetNode) return;
      const ordered = sourceNode.id < targetNode.id ? [sourceNode, targetNode] : [targetNode, sourceNode];
      const key = `${ordered[0].id}-${ordered[1].id}`;
      let entry = grouped.get(key);
      if (!entry) {
        entry = {
          sourceId: ordered[0].id,
          targetId: ordered[1].id,
          isBroadcast: ordered[0].id === BROADCAST_ID || ordered[1].id === BROADCAST_ID,
          linkRefs: [],
        };
        grouped.set(key, entry);
      }
      entry.linkRefs.push(link);
    });
    aggregatedLinkGroups = Array.from(grouped.values());
  }

  function refreshHoverIndex(now) {
    if (now - hoverIndexBuiltAt < HOVER_INDEX_REFRESH_MS && nodes.length === hoverIndexNodeCount) {
      return;
    }
    const candidates = nodes.filter(
      (node) => Number.isFinite(node.x) && Number.isFinite(node.y),
    );
    hoverIndex = d3.quadtree()
      .x((node) => node.x)
      .y((node) => node.y)
      .addAll(candidates);
    hoverIndexBuiltAt = now;
    hoverIndexNodeCount = nodes.length;
  }

  function findNode(x, y) {
    const now = performance.now();
    refreshHoverIndex(now);
    if (hoverIndex) {
      const candidate = hoverIndex.find(x, y, HOVER_MAX_RADIUS);
      if (candidate) {
        const radius = 6 + Math.log1p(candidate.count || 1);
        const dx = candidate.x - x;
        const dy = candidate.y - y;
        if (Math.hypot(dx, dy) <= radius + 4) {
          return candidate;
        }
      }
    }
    return null;
  }

  function resolveNode(ref) {
    if (ref === null || ref === undefined) return null;
    if (typeof ref === "object") return ref;
    return nodeIndex.get(ref);
  }

  function makeBoundsForce() {
    let nodesLocal = [];
    function force() {
      const margin = padding;
      const minX = margin;
      const maxX = width - margin;
      const minY = margin;
      const maxY = height - margin;
      for (const node of nodesLocal) {
        if (node.x === undefined || node.y === undefined) {
          continue;
        }
        if (node.x < minX) {
          node.vx += (minX - node.x) * 0.01;
        } else if (node.x > maxX) {
          node.vx -= (node.x - maxX) * 0.01;
        }
        if (node.y < minY) {
          node.vy += (minY - node.y) * 0.01;
        } else if (node.y > maxY) {
          node.vy -= (node.y - maxY) * 0.01;
        }
      }
    }
    force.initialize = (nodesInput) => {
      nodesLocal = nodesInput;
    };
    return force;
  }

  function makeHomeForce() {
    let nodesLocal = [];
    function force(alpha) {
      for (const node of nodesLocal) {
        if (!node.home || node.x === undefined || node.y === undefined) {
          continue;
        }
        node.vx += (node.home.x - node.x) * 0.04 * alpha;
        node.vy += (node.home.y - node.y) * 0.04 * alpha;
      }
    }
    force.initialize = (nodesInput) => {
      nodesLocal = nodesInput;
    };
    return force;
  }

  function computeLayoutSpacing() {
    const size = Math.max(1, Math.min(width, height));
    const nodeCount = Math.max(1, nodes.length);
    const density = Math.sqrt(nodeCount);
    const base = Math.max(220, size * 0.34);
    const linkDistance = Math.min(520, Math.max(240, base + density * 8));
    const charge = -Math.min(1700, Math.max(700, linkDistance * 2.2));
    const collision = Math.min(54, Math.max(24, linkDistance / 7.2));
    return { linkDistance, charge, collision };
  }

  function applyDynamicForces() {
    if (!simulation) return;
    const layout = computeLayoutSpacing();
    const linkForce = simulation.force("link");
    if (linkForce && linkForce.distance) {
      linkForce
        .distance(layout.linkDistance)
        .strength(0.08);
    }
    const chargeForce = simulation.force("charge");
    if (chargeForce && chargeForce.strength) {
      chargeForce.strength(layout.charge);
    }
    const collisionForce = simulation.force("collision");
    if (collisionForce && collisionForce.radius) {
      collisionForce.radius((d) => layout.collision + Math.log1p(d.count || 1) * 4);
    }
  }

  function decayActivity(now) {
    if (!activityUpdatedAt) {
      activityUpdatedAt = now;
      return { energy: activityEnergy, spike: activitySpike, combined: activityEnergy + activitySpike };
    }
    const age = now - activityUpdatedAt;
    if (age > 0) {
      activityEnergy *= Math.exp(-age / ACTIVITY_DECAY_MS);
      activitySpike *= Math.exp(-age / ACTIVITY_SPIKE_DECAY_MS);
      activityUpdatedAt = now;
    }
    const energy = Math.min(1, activityEnergy);
    const spike = Math.min(1, activitySpike);
    const combined = Math.min(1.4, energy + spike * 0.8);
    return { energy, spike, combined };
  }

  function bumpActivity(strength = 1) {
    const now = performance.now();
    decayActivity(now);
    const scaled = Math.max(0, strength);
    activityEnergy = Math.min(1.6, activityEnergy + 0.16 * scaled);
    activitySpike = Math.min(1, activitySpike + 0.35 * scaled);
    activityUpdatedAt = now;
  }

  function drawParallax(now) {
    if (!width || !height) return;
    const t = now * 0.00004;
    const parallaxX = -transform.x * 0.05;
    const parallaxY = -transform.y * 0.05;
    const maxRadius = Math.max(width, height);
    const layers = [
      {
        x: width * 0.2 + Math.cos(t * 0.9) * width * 0.08,
        y: height * 0.18 + Math.sin(t * 1.1) * height * 0.07,
        radius: maxRadius * 0.9,
        color: "rgba(90, 216, 200, 0.06)",
        factor: 0.3,
      },
      {
        x: width * 0.82 + Math.sin(t * 0.7 + 1.4) * width * 0.07,
        y: height * 0.22 + Math.cos(t * 0.8 + 2.2) * height * 0.06,
        radius: maxRadius * 0.75,
        color: "rgba(122, 169, 255, 0.05)",
        factor: 0.45,
      },
      {
        x: width * 0.55 + Math.cos(t * 0.5 + 3.1) * width * 0.06,
        y: height * 0.78 + Math.sin(t * 0.6 + 4.2) * height * 0.06,
        radius: maxRadius * 0.85,
        color: "rgba(247, 201, 75, 0.04)",
        factor: 0.6,
      },
    ];

    ctx.save();
    ctx.globalCompositeOperation = "source-over";
    layers.forEach((layer) => {
      const cx = layer.x + parallaxX * layer.factor;
      const cy = layer.y + parallaxY * layer.factor;
      const gradient = ctx.createRadialGradient(cx, cy, 0, cx, cy, layer.radius);
      gradient.addColorStop(0, layer.color);
      gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = gradient;
      ctx.fillRect(0, 0, width, height);
    });
    ctx.restore();
  }

  function drawNodeHeat(now) {
    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    nodes.forEach((node) => {
      if (!node || node.x === undefined || node.y === undefined) {
        return;
      }
      const heatAge = node.lastHeatAt ? now - node.lastHeatAt : Number.POSITIVE_INFINITY;
      const heat = node.heat ? node.heat * Math.exp(-heatAge / NODE_HEAT_HALF_LIFE_MS) : 0;
      if (heat <= 0.02) {
        return;
      }
      const boost = node.isBroadcast ? 0.6 : 1;
      const intensity = Math.min(1, (heat / NODE_HEAT_MAX) * boost);
      const radius = 16 + intensity * 30 + Math.log1p(node.count || 1) * 2;
      const color = node.color || colorForNode(node.id);
      const gradient = ctx.createRadialGradient(node.x, node.y, 0, node.x, node.y, radius);
      gradient.addColorStop(0, rgbaFromHex(color, 0.22 * intensity));
      gradient.addColorStop(0.4, rgbaFromHex(color, 0.12 * intensity));
      gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = gradient;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
      ctx.fill();
    });
    ctx.restore();
  }

  function resolvePulsePosition(pulse) {
    if (pulse.nodeId !== null && pulse.nodeId !== undefined) {
      const node = nodeIndex.get(pulse.nodeId);
      if (!node) {
        return null;
      }
      if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) {
        return null;
      }
      return { x: node.x, y: node.y };
    }
    if (pulse.sourceId !== null && pulse.sourceId !== undefined &&
        pulse.targetId !== null && pulse.targetId !== undefined) {
      const source = nodeIndex.get(pulse.sourceId);
      const target = nodeIndex.get(pulse.targetId);
      if (!source || !target) {
        return null;
      }
      if (!Number.isFinite(source.x) || !Number.isFinite(source.y) ||
          !Number.isFinite(target.x) || !Number.isFinite(target.y)) {
        return null;
      }
      return { x: (source.x + target.x) / 2, y: (source.y + target.y) / 2 };
    }
    if (Number.isFinite(pulse.x) && Number.isFinite(pulse.y)) {
      return { x: pulse.x, y: pulse.y };
    }
    return null;
  }

  function trimBubbleLine(line, maxWidth, suffix = "") {
    let value = line;
    if (!value) return suffix ? suffix.trim() : "";
    while (value.length > 0 && ctx.measureText(`${value}${suffix}`).width > maxWidth) {
      value = value.slice(0, -1);
    }
    return `${value}${suffix}`;
  }

  function wrapBubbleLines(text) {
    const words = String(text).split(" ");
    const lines = [];
    let line = "";
    words.forEach((word) => {
      const candidate = line ? `${line} ${word}` : word;
      if (ctx.measureText(candidate).width <= TEXT_BUBBLE_MAX_WIDTH || !line) {
        line = candidate;
      } else {
        lines.push(line);
        line = word;
      }
    });
    if (line) {
      lines.push(line);
    }
    let truncated = false;
    if (lines.length > TEXT_BUBBLE_MAX_LINES) {
      truncated = true;
      lines.length = TEXT_BUBBLE_MAX_LINES;
    }
    return lines.map((value, idx) => {
      if (truncated && idx === lines.length - 1) {
        return trimBubbleLine(value, TEXT_BUBBLE_MAX_WIDTH, "...");
      }
      return trimBubbleLine(value, TEXT_BUBBLE_MAX_WIDTH);
    });
  }

  function addMessageBubble(nodeId, text, options = {}) {
    if (nodeId === null || nodeId === undefined) return;
    const message = String(text || "").replace(/\s+/g, " ").trim();
    if (!message) return;
    const now = performance.now();
    ctx.save();
    ctx.font = "12px IBM Plex Mono";
    const lines = wrapBubbleLines(message);
    ctx.restore();
    if (!lines.length) return;
    let contentWidth = 0;
    ctx.save();
    ctx.font = "12px IBM Plex Mono";
    lines.forEach((line) => {
      contentWidth = Math.max(contentWidth, ctx.measureText(line).width);
    });
    ctx.restore();
    contentWidth = Math.min(TEXT_BUBBLE_MAX_WIDTH, Math.max(40, contentWidth));
    const width = contentWidth + TEXT_BUBBLE_PADDING_X * 2;
    const height = lines.length * TEXT_BUBBLE_LINE_HEIGHT + TEXT_BUBBLE_PADDING_Y * 2;
    const color = options.color || colorForNode(nodeId);
    if (!messageBubbles.has(nodeId) && messageBubbles.size >= TEXT_BUBBLE_MAX) {
      let oldestId = null;
      let oldestAt = Infinity;
      messageBubbles.forEach((bubble, id) => {
        if (bubble.start < oldestAt) {
          oldestAt = bubble.start;
          oldestId = id;
        }
      });
      if (oldestId !== null) {
        messageBubbles.delete(oldestId);
      }
    }
    messageBubbles.set(nodeId, {
      nodeId,
      lines,
      width,
      height,
      color,
      start: now,
      duration: TEXT_BUBBLE_MS,
    });
  }

  function drawRoundedRect(x, y, width, height, radius) {
    const r = Math.min(radius, width / 2, height / 2);
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + width, y, x + width, y + height, r);
    ctx.arcTo(x + width, y + height, x, y + height, r);
    ctx.arcTo(x, y + height, x, y, r);
    ctx.arcTo(x, y, x + width, y, r);
    ctx.closePath();
  }

  function drawMessageBubbles(now) {
    if (!messageBubbles.size) return;
    const expired = [];
    ctx.save();
    ctx.font = "12px IBM Plex Mono";
    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    messageBubbles.forEach((bubble, nodeId) => {
      const node = nodeIndex.get(nodeId);
      if (!node || !Number.isFinite(node.x) || !Number.isFinite(node.y)) {
        return;
      }
      const age = now - bubble.start;
      if (age >= bubble.duration) {
        expired.push(nodeId);
        return;
      }
      const fade = 1 - age / bubble.duration;
      const radius = 5 + Math.log1p(node.count || 1);
      const bubbleWidth = bubble.width;
      const bubbleHeight = bubble.height;
      let x = node.x - bubbleWidth / 2;
      let y = node.y - radius - bubbleHeight - 12;
      let tailUp = false;
      if (y < 10) {
        y = node.y + radius + 12;
        tailUp = true;
      }
      x = clampValue(x, 8, width - bubbleWidth - 8);
      y = clampValue(y, 8, height - bubbleHeight - 8);
      const color = bubble.color || colorForNode(nodeId);

      ctx.globalAlpha = 0.9 * fade;
      ctx.fillStyle = "rgba(8, 14, 20, 0.88)";
      ctx.strokeStyle = rgbaFromHex(color, 0.5 * fade);
      ctx.lineWidth = 1;
      ctx.shadowColor = rgbaFromHex(color, 0.35 * fade);
      ctx.shadowBlur = 12;
      drawRoundedRect(x, y, bubbleWidth, bubbleHeight, 10);
      ctx.fill();
      ctx.stroke();

      const tailX = clampValue(node.x, x + 14, x + bubbleWidth - 14);
      ctx.beginPath();
      if (tailUp) {
        ctx.moveTo(tailX - 6, y);
        ctx.lineTo(tailX + 6, y);
        ctx.lineTo(tailX, y - 8);
      } else {
        ctx.moveTo(tailX - 6, y + bubbleHeight);
        ctx.lineTo(tailX + 6, y + bubbleHeight);
        ctx.lineTo(tailX, y + bubbleHeight + 8);
      }
      ctx.closePath();
      ctx.fill();
      ctx.stroke();

      ctx.shadowBlur = 0;
      ctx.fillStyle = `rgba(233, 241, 247, ${0.95 * fade})`;
      bubble.lines.forEach((line, idx) => {
        ctx.fillText(
          line,
          x + TEXT_BUBBLE_PADDING_X,
          y + TEXT_BUBBLE_PADDING_Y + idx * TEXT_BUBBLE_LINE_HEIGHT,
        );
      });
    });
    ctx.restore();
    expired.forEach((nodeId) => {
      messageBubbles.delete(nodeId);
    });
  }

  function emitRipple(origin, portnum, options = {}) {
    if (!origin) {
      return;
    }
    const now = performance.now();
    const activity = decayActivity(now);
    const stormScale = options.stormScale ?? 1;
    const storm = Math.min(1, activity.combined) * stormScale;
    const baseRings = options.ringCount || 1;
    const stormRings = options.stormRings ? (storm > 0.55 ? 1 : 0) : 0;
    const ringCount = Math.min(3, baseRings + stormRings);
    const ringSpacing = options.ringSpacing ?? RIPPLE_RING_SPACING;
    const ringStagger = options.ringStagger ?? RIPPLE_RING_STAGGER_MS;
    const duration = options.duration ?? 900;
    const maxRadius = options.maxRadius ?? 45;
    const lineWidth = options.lineWidth ?? 2;
    const alpha = options.alpha ?? 0.5;
    const color = options.color || colorForPort(portnum);
    const intensity = options.intensity ?? 1;
    const radiusScale = 1 + storm * 0.15;
    const alphaScale = 0.85 + storm * 0.4;

    for (let i = 0; i < ringCount; i += 1) {
      const offset = (i - (ringCount - 1) / 2) * ringSpacing;
      const fade = 1 - i * 0.16;
      pulses.push({
        nodeId: origin.nodeId ?? null,
        sourceId: origin.sourceId ?? null,
        targetId: origin.targetId ?? null,
        x: origin.x,
        y: origin.y,
        portnum,
        kind: options.kind || "default",
        color,
        start: now + i * ringStagger,
        duration: duration * (1 + i * 0.04),
        maxRadius: (maxRadius + offset) * radiusScale,
        lineWidth: lineWidth * (1 - i * 0.1) * (0.9 + storm * 0.15),
        alpha: alpha * fade * alphaScale * intensity,
        glow: options.glow !== undefined ? options.glow : true,
      });
    }
  }

  function drawArrowhead(x1, y1, x2, y2, t, size, color, alpha) {
    const dx = x2 - x1;
    const dy = y2 - y1;
    const distance = Math.hypot(dx, dy);
    if (!Number.isFinite(distance) || distance < size * 2) {
      return;
    }
    const tipX = x1 + dx * t;
    const tipY = y1 + dy * t;
    const angle = Math.atan2(dy, dx);
    const back = size * 1.4;
    const wing = size * 0.7;
    ctx.save();
    ctx.translate(tipX, tipY);
    ctx.rotate(angle);
    ctx.globalAlpha = alpha;
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.moveTo(0, 0);
    ctx.lineTo(-back, wing);
    ctx.lineTo(-back, -wing);
    ctx.closePath();
    ctx.fill();
    ctx.restore();
  }

  function setSelectedFocus(payload) {
    const now = performance.now();
    if (!payload || payload.nodeId === null || payload.nodeId === undefined) {
      if (selectedFocus && !selectedFocus.releasedAt) {
        selectedFocus.releasedAt = now;
      }
      return;
    }
    const sameNode = selectedFocus && selectedFocus.nodeId === payload.nodeId;
    const keepLinks = sameNode && !Object.prototype.hasOwnProperty.call(payload, "links");
    const keepRoutes = sameNode && !Object.prototype.hasOwnProperty.call(payload, "routes");
    selectedFocus = {
      nodeId: payload.nodeId,
      links: keepLinks ? selectedFocus.links : payload.links || [],
      routes: keepRoutes ? selectedFocus.routes : payload.routes || [],
      startedAt: sameNode ? selectedFocus.startedAt : now,
      releasedAt: null,
    };
  }

  function drawSelectedLinks(links, fade) {
    if (!links || !links.length) return;
    ctx.save();
    ctx.globalCompositeOperation = "screen";
    links.forEach((link) => {
      const source = nodeIndex.get(link.sourceId);
      const target = nodeIndex.get(link.targetId);
      if (!source || !target) return;
      if (!Number.isFinite(source.x) || !Number.isFinite(source.y) ||
          !Number.isFinite(target.x) || !Number.isFinite(target.y)) {
        return;
      }
      const color = colorForPort(link.portnum);
      const count = link.count || 1;
      const baseWidth = 1.6 + Math.log1p(count) * 0.9;
      const alpha = Math.min(0.85, 0.35 + Math.log1p(count) * 0.12);

      ctx.strokeStyle = color;
      ctx.globalAlpha = alpha * fade;
      ctx.lineWidth = baseWidth + 4;
      ctx.shadowColor = color;
      ctx.shadowBlur = 18;
      ctx.beginPath();
      ctx.moveTo(source.x, source.y);
      ctx.lineTo(target.x, target.y);
      ctx.stroke();

      ctx.globalAlpha = Math.min(1, alpha + 0.2) * fade;
      ctx.lineWidth = baseWidth;
      ctx.shadowBlur = 10;
      ctx.beginPath();
      ctx.moveTo(source.x, source.y);
      ctx.lineTo(target.x, target.y);
      ctx.stroke();
    });
    ctx.restore();
  }

  function drawSelectedRoutes(routes, fade) {
    if (!routes || !routes.length) return;
    const arrowSize = 5 / transform.k;
    ctx.save();
    ctx.globalCompositeOperation = "screen";
    routes.forEach((route) => {
      const path = route.path || [];
      if (path.length < 2) return;
      const color = route.color || ROUTE_COLOR_FORWARD;
      ctx.strokeStyle = color;
      ctx.lineWidth = 2.2;
      ctx.globalAlpha = 0.55 * fade;
      ctx.shadowColor = color;
      ctx.shadowBlur = 16;
      for (let idx = 0; idx < path.length - 1; idx += 1) {
        const source = nodeIndex.get(path[idx]);
        const target = nodeIndex.get(path[idx + 1]);
        if (!source || !target) continue;
        if (!Number.isFinite(source.x) || !Number.isFinite(source.y) ||
            !Number.isFinite(target.x) || !Number.isFinite(target.y)) {
          continue;
        }
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.lineTo(target.x, target.y);
        ctx.stroke();
        drawArrowhead(source.x, source.y, target.x, target.y, 0.72, arrowSize, color, 0.75 * fade);
      }
    });
    ctx.restore();
  }

  function drawSelectedFocus(now) {
    if (!selectedFocus) return;
    let fade = 1;
    if (selectedFocus.releasedAt) {
      fade = 1 - Math.min(1, (now - selectedFocus.releasedAt) / SELECTION_FADE_MS);
      if (fade <= 0) {
        selectedFocus = null;
        return;
      }
    }
    drawSelectedLinks(selectedFocus.links, fade);
    drawSelectedRoutes(selectedFocus.routes, fade);
  }

  const rippleProfiles = {
    default: {
      duration: 1000,
      maxRadius: 68,
      lineWidth: 2.4,
      alpha: 0.72,
      ringCount: 1,
      ringSpacing: RIPPLE_RING_SPACING,
      ringStagger: RIPPLE_RING_STAGGER_MS,
      stormRings: true,
      stormScale: 1,
      glow: true,
    },
    send: {
      duration: 1000,
      maxRadius: 68,
      lineWidth: 2.4,
      alpha: 0.72,
      ringCount: 1,
      ringSpacing: RIPPLE_RING_SPACING,
      ringStagger: RIPPLE_RING_STAGGER_MS,
      stormRings: true,
      stormScale: 1,
      glow: true,
    },
    receive: {
      duration: 1040,
      maxRadius: 62,
      lineWidth: 2.2,
      alpha: 0.68,
      ringCount: 1,
      ringSpacing: RIPPLE_RING_SPACING,
      ringStagger: RIPPLE_RING_STAGGER_MS,
      stormRings: true,
      stormScale: 0.9,
      glow: true,
    },
    broadcast: {
      duration: 1600,
      maxRadius: 78,
      lineWidth: 1.6,
      alpha: 0.42,
      ringCount: 2,
      ringSpacing: 7,
      ringStagger: 90,
      stormRings: true,
      stormScale: 0.6,
      glow: false,
    },
    "broadcast-core": {
      duration: 1800,
      maxRadius: 104,
      lineWidth: 1.4,
      alpha: 0.3,
      ringCount: 2,
      ringSpacing: 8,
      ringStagger: 110,
      stormRings: false,
      stormScale: 0.3,
      glow: false,
    },
    route: {
      duration: 1200,
      maxRadius: 56,
      lineWidth: 2.6,
      alpha: 0.72,
      ringCount: 1,
      ringSpacing: 6,
      ringStagger: 70,
      stormRings: false,
      stormScale: 0.6,
      glow: true,
    },
  };

  function drawRouteAnimations(now) {
    for (let i = routeAnimations.length - 1; i >= 0; i -= 1) {
      const anim = routeAnimations[i];
      const elapsed = now - anim.start;
      const totalSegments = anim.path.length - 1;
      const totalDuration = totalSegments * anim.stepMs;
      const fadeElapsed = Math.max(0, elapsed - totalDuration);
      const fadeFactor = fadeElapsed > 0 ? 1 - Math.min(1, fadeElapsed / anim.fadeMs) : 1;
      if (fadeFactor <= 0) {
        routeAnimations.splice(i, 1);
        continue;
      }

      const progress = Math.min(totalSegments, Math.max(0, elapsed / anim.stepMs));
      const wholeSegments = Math.floor(progress);
      const activeIndex = Math.min(totalSegments - 1, wholeSegments);
      const activeT = progress >= totalSegments ? 1 : progress - wholeSegments;
      const color = anim.color || ROUTE_COLOR_FORWARD;

      ctx.save();
      ctx.strokeStyle = color;
      ctx.lineWidth = 3;
      ctx.globalAlpha = 0.75 * fadeFactor;
      ctx.shadowColor = color;
      ctx.shadowBlur = 14;

      for (let idx = 0; idx <= activeIndex; idx += 1) {
        const source = nodeIndex.get(anim.path[idx]);
        const target = nodeIndex.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        let endX = target.x;
        let endY = target.y;
        if (idx === activeIndex && activeIndex < totalSegments) {
          endX = source.x + (target.x - source.x) * activeT;
          endY = source.y + (target.y - source.y) * activeT;
        }
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.lineTo(endX, endY);
        ctx.stroke();
      }

      const arrowSize = 6 / transform.k;
      for (let idx = 0; idx <= activeIndex; idx += 1) {
        const source = nodeIndex.get(anim.path[idx]);
        const target = nodeIndex.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        if (idx === activeIndex && activeIndex < totalSegments) {
          if (activeT < 0.2) {
            continue;
          }
          const arrowT = Math.min(0.9, activeT * 0.85);
          drawArrowhead(source.x, source.y, target.x, target.y, arrowT, arrowSize, color, 0.85 * fadeFactor);
        } else {
          drawArrowhead(source.x, source.y, target.x, target.y, 0.78, arrowSize, color, 0.7 * fadeFactor);
        }
      }

      ctx.shadowBlur = 0;
      ctx.globalAlpha = 0.4 * fadeFactor;
      ctx.lineWidth = 2;
      ctx.setLineDash([6, 8]);
      for (let idx = 0; idx < activeIndex; idx += 1) {
        const source = nodeIndex.get(anim.path[idx]);
        const target = nodeIndex.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.lineTo(target.x, target.y);
        ctx.stroke();
      }
      ctx.setLineDash([]);

      ctx.globalAlpha = 0.65 * fadeFactor;
      for (let idx = 0; idx <= activeIndex; idx += 1) {
        const node = nodeIndex.get(anim.path[idx]);
        if (!node) continue;
        ctx.beginPath();
        ctx.arc(node.x, node.y, 3.5, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.fill();
      }
      ctx.restore();

      ctx.save();
      ctx.globalAlpha = 0.9 * fadeFactor;
      ctx.fillStyle = rgbaFromHex(color, 0.9);
      ctx.shadowColor = rgbaFromHex(color, 0.75);
      ctx.shadowBlur = 12;
      const sparkleCount = 2;
      for (let idx = 0; idx <= activeIndex; idx += 1) {
        const source = nodeIndex.get(anim.path[idx]);
        const target = nodeIndex.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        const segEndT = idx === activeIndex ? activeT : 1;
        for (let s = 0; s < sparkleCount; s += 1) {
          const phase = (now / 220 + idx * 0.37 + s * 0.45) % 1;
          const t = Math.min(segEndT, phase);
          if (t <= 0) continue;
          const x = source.x + (target.x - source.x) * t;
          const y = source.y + (target.y - source.y) * t;
          const twinkle = 0.6 + 0.4 * Math.sin((now / 120) + idx + s);
          ctx.beginPath();
          ctx.globalAlpha = 0.7 * fadeFactor * twinkle;
          ctx.arc(x, y, 2.2 + twinkle, 0, Math.PI * 2);
          ctx.fill();
        }
      }
      ctx.restore();

      ctx.save();
      ctx.font = "10px Space Grotesk";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      for (let idx = 0; idx <= activeIndex; idx += 1) {
        const source = nodeIndex.get(anim.path[idx]);
        const target = nodeIndex.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        const segEndT = idx === activeIndex ? activeT : 1;
        const t = Math.max(0.2, Math.min(0.8, segEndT * 0.6 + 0.2));
        const x = source.x + (target.x - source.x) * t;
        const y = source.y + (target.y - source.y) * t - 10;
        const radius = 7;
        const label = String(idx + 1);
        ctx.globalAlpha = 0.85 * fadeFactor;
        ctx.fillStyle = "rgba(11, 16, 22, 0.85)";
        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fill();
        ctx.strokeStyle = color;
        ctx.lineWidth = 1;
        ctx.stroke();
        ctx.fillStyle = "rgba(255, 255, 255, 0.95)";
        ctx.fillText(label, x, y + 0.5);
      }
      ctx.restore();
    }
  }

  function draw() {
    const now = performance.now();
    const frameStart = now;
    const nowEpoch = Date.now() / 1000;
    ctx.clearRect(0, 0, width, height);
    drawParallax(now);
    ctx.save();
    ctx.translate(transform.x, transform.y);
    ctx.scale(transform.k, transform.k);

    const broadcastLinks = [];
    const rfLinks = [];
    aggregatedLinkGroups.forEach((group) => {
      const sourceNode = nodeIndex.get(group.sourceId);
      const targetNode = nodeIndex.get(group.targetId);
      if (!sourceNode || !targetNode) return;
      let count = 0;
      let lastSeen = 0;
      let portnum = null;
      let heat = 0;
      let flashUntil = 0;
      group.linkRefs.forEach((link) => {
        count += link.count || 0;
        flashUntil = Math.max(flashUntil, link.flashUntil || 0);
        const heatAge = link.lastHeatAt ? now - link.lastHeatAt : 0;
        const linkHeat = link.heat ? link.heat * Math.exp(-heatAge / LINK_HEAT_HALF_LIFE_MS) : 0;
        heat = Math.min(LINK_HEAT_MAX, heat + linkHeat);
        const candidateSeen = link.lastSeen || 0;
        if (candidateSeen >= lastSeen) {
          lastSeen = candidateSeen;
          portnum = link.portnum;
        }
      });
      const entry = {
        sourceNode,
        targetNode,
        count,
        lastSeen,
        portnum,
        heat,
        flashUntil,
        isBroadcast: group.isBroadcast,
      };
      if (group.isBroadcast) {
        broadcastLinks.push(entry);
      } else {
        rfLinks.push(entry);
      }
    });

    const drawLink = (entry, isBroadcastLink) => {
      const sourceNode = entry.sourceNode;
      const targetNode = entry.targetNode;
      if (!sourceNode || !targetNode) return;
      const color = colorForPort(entry.portnum);
      const flashRemaining = entry.flashUntil
        ? Math.max(0, Math.min(1, (entry.flashUntil - now) / LINK_FLASH_MS))
        : 0;
      const ageMs = entry.lastSeen ? (nowEpoch - entry.lastSeen) * 1000 : Number.POSITIVE_INFINITY;
      const fadeFactor = entry.lastSeen ? Math.max(0, Math.min(1, 1 - ageMs / LINK_FADE_MS)) : 0;
      const heatNormalized = entry.heat ? Math.min(1, entry.heat / LINK_HEAT_MAX) : 0;
      const heatVisibility = Math.max(fadeFactor, heatNormalized);
      const visibility = Math.max(heatVisibility, flashRemaining);
      if (visibility <= 0.01) {
        return;
      }
      const baseWidth = (isBroadcastLink ? 0.7 : 1.2) + Math.log1p(entry.count || 1) * 0.6;
      const baseAlpha = isBroadcastLink ? 0.05 : 0.28;
      const flashAlpha = isBroadcastLink ? 0.8 : 0.95;
      const alpha =
        (baseAlpha + (flashAlpha - baseAlpha) * flashRemaining) * (flashRemaining > 0 ? 1 : heatVisibility);
      const width = baseWidth + (isBroadcastLink ? 2 : 3) * flashRemaining;
      const strokePath = () => {
        ctx.beginPath();
        ctx.moveTo(sourceNode.x, sourceNode.y);
        ctx.lineTo(targetNode.x, targetNode.y);
        ctx.stroke();
      };

      if (!isBroadcastLink && flashRemaining > 0.05) {
        ctx.save();
        ctx.strokeStyle = color;
        ctx.globalAlpha = alpha;
        ctx.lineWidth = width + 2;
        ctx.shadowColor = color;
        ctx.shadowBlur = 12 + flashRemaining * 14;
        strokePath();
        ctx.restore();
      }

      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = alpha;
      ctx.lineWidth = width;
      ctx.setLineDash(isBroadcastLink ? [6, 10] : []);
      strokePath();
      ctx.restore();

      if (!isBroadcastLink && flashRemaining > 0.05) {
        const t = 1 - flashRemaining;
        const point = {
          x: sourceNode.x + (targetNode.x - sourceNode.x) * t,
          y: sourceNode.y + (targetNode.y - sourceNode.y) * t,
        };
        ctx.save();
        ctx.fillStyle = color;
        ctx.globalAlpha = 0.85 * flashRemaining;
        ctx.beginPath();
        ctx.arc(point.x, point.y, 3.5 + flashRemaining * 2, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
      }
    };

    broadcastLinks.forEach((entry) => drawLink(entry, true));
    rfLinks.forEach((entry) => drawLink(entry, false));

    drawSelectedFocus(now);
    drawRouteAnimations(now);
    drawNodeHeat(now);

    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    for (let i = pulses.length - 1; i >= 0; i -= 1) {
      const pulse = pulses[i];
      const pos = resolvePulsePosition(pulse);
      if (!pos) {
        pulses.splice(i, 1);
        continue;
      }
      const t = (now - pulse.start) / pulse.duration;
      if (t >= 1) {
        pulses.splice(i, 1);
        continue;
      }
      if (t < 0) {
        continue;
      }
      const fade = 1 - t;
      const radius = 8 + t * (pulse.maxRadius || 42);
      ctx.strokeStyle = pulse.color;
      ctx.globalAlpha = (pulse.alpha || 0.5) * fade;
      ctx.lineWidth = (pulse.lineWidth || 2) * (0.85 + 0.2 * fade);
      if (pulse.glow) {
        ctx.shadowColor = pulse.color;
        ctx.shadowBlur = 8 + 12 * fade;
      } else {
        ctx.shadowBlur = 0;
      }
      ctx.beginPath();
      ctx.arc(pos.x, pos.y, radius, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.restore();

    nodes.forEach((node) => {
      if (node.x === undefined || node.y === undefined) {
        return;
      }
      const radius = 5 + Math.log1p(node.count || 1);
      const nodeColor = node.color || colorForNode(node.id);
      const isBroadcast = node.id === BROADCAST_ID || node.isBroadcast;
      if (isBroadcast) {
        const pulseRadius = radius + 10;
        if (logoImage.complete && logoImage.naturalWidth) {
          const size = Math.max(140, radius * 12);
          ctx.save();
          ctx.globalAlpha = 0.28;
          ctx.drawImage(logoImage, node.x - size / 2, node.y - size / 2, size, size);
          ctx.restore();
        }
        ctx.globalAlpha = 1;
        ctx.fillStyle = rgbaFromHex(nodeColor, 0.14);
        ctx.beginPath();
        ctx.arc(node.x, node.y, pulseRadius + 10, 0, Math.PI * 2);
        ctx.fill();

        ctx.strokeStyle = rgbaFromHex(nodeColor, 0.55);
        ctx.lineWidth = 1.8;
        ctx.beginPath();
        ctx.arc(node.x, node.y, pulseRadius, 0, Math.PI * 2);
        ctx.stroke();

        ctx.fillStyle = rgbaFromHex(nodeColor, 0.3);
        ctx.beginPath();
        ctx.arc(node.x, node.y, radius + 4, 0, Math.PI * 2);
        ctx.fill();

        ctx.fillStyle = rgbaFromHex(nodeColor, 0.85);
        ctx.beginPath();
        ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
        ctx.fill();

        ctx.fillStyle = "rgba(200, 220, 235, 0.95)";
        ctx.font = "11px Space Grotesk";
        ctx.fillText("broadcast", node.x + radius + 6, node.y + 3);
        return;
      }
      const lastSeenAge = node.lastSeenEpoch ? nowEpoch - node.lastSeenEpoch : null;

      if (lastSeenAge !== null) {
        let ringColor = null;
        if (lastSeenAge < 120) {
          ringColor = rgbaFromHex(nodeColor, 0.18);
        } else if (lastSeenAge < 600) {
          ringColor = rgbaFromHex(nodeColor, 0.1);
        }
        if (ringColor) {
          ctx.fillStyle = ringColor;
          ctx.beginPath();
          ctx.arc(node.x, node.y, radius + 5, 0, Math.PI * 2);
          ctx.fill();
        }
      }

      const sentAge = node.lastSentAt ? now - node.lastSentAt : null;
      if (sentAge !== null && sentAge < SEND_FLASH_MS) {
        const t = sentAge / SEND_FLASH_MS;
        const ringColor = node.lastSendColor || nodeColor;
        ctx.strokeStyle = rgbaFromHex(ringColor, 0.55 * (1 - t));
        ctx.lineWidth = 1.4;
        ctx.beginPath();
        ctx.arc(node.x, node.y, radius + 6 + t * 6, 0, Math.PI * 2);
        ctx.stroke();
      }

      const receiveAge = node.lastReceivedAt ? now - node.lastReceivedAt : null;
      if (receiveAge !== null && receiveAge < RECEIVE_FLASH_MS) {
        const t = receiveAge / RECEIVE_FLASH_MS;
        const ringColor = node.lastReceiveColor || nodeColor;
        ctx.strokeStyle = rgbaFromHex(ringColor, 0.5 * (1 - t));
        ctx.lineWidth = 1.3;
        ctx.beginPath();
        ctx.arc(node.x, node.y, radius + 10 + t * 6, 0, Math.PI * 2);
        ctx.stroke();
      }

      ctx.globalAlpha = 1;
      ctx.fillStyle = rgbaFromHex(nodeColor, 0.85);
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius, 0, Math.PI * 2);
      ctx.fill();

      ctx.strokeStyle = rgbaFromHex(nodeColor, 0.55);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.arc(node.x, node.y, radius + 1.2, 0, Math.PI * 2);
      ctx.stroke();

      ctx.fillStyle = "rgba(200, 220, 235, 0.95)";
      ctx.font = "11px Space Grotesk";
      ctx.fillText(node.label || node.id, node.x + radius + 5, node.y + 3);
    });

    drawMessageBubbles(now);

    ctx.restore();
    recordPerfFrame(perfStats, now, performance.now() - frameStart);
    requestAnimationFrame(draw);
  }

  function init() {
    resize();
    homeForce = makeHomeForce();

    simulation = d3.forceSimulation(nodes)
      .force("charge", d3.forceManyBody().strength(-740))
      .force("link", d3.forceLink(links).id((d) => d.id).distance(360).strength(0.08))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("home", homeForce)
      .force(
        "collision",
        d3.forceCollide().radius((d) => 26 + Math.log1p(d.count || 1) * 4).iterations(2),
      )
      .force("bounds", makeBoundsForce())
      .alphaDecay(0.18)
      .velocityDecay(0.7);
    applyDynamicForces();

    simulation.on("tick", () => {
      if (!nodes.length) return;
    });

    const zoom = d3.zoom()
      .scaleExtent([0.3, 2.6])
      .on("start", () => {
        if (onBackgroundAction) {
          onBackgroundAction();
        }
      })
      .on("zoom", (event) => {
        transform = event.transform;
      });

    const drag = d3.drag()
      .subject((event) => {
        const [x, y] = transform.invert([event.x, event.y]);
        return findNode(x, y);
      })
      .on("start", (event) => {
        if (!event.subject) return;
        event.sourceEvent.stopPropagation();
        simulation.alphaTarget(0.3).restart();
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
      })
      .on("drag", (event) => {
        if (!event.subject) return;
        const [x, y] = transform.invert([event.x, event.y]);
        event.subject.fx = x;
        event.subject.fy = y;
      })
      .on("end", (event) => {
        if (!event.subject) return;
        simulation.alphaTarget(0);
        event.subject.fx = event.subject.x;
        event.subject.fy = event.subject.y;
      });

    const canvasSelection = d3.select(canvas);
    canvasSelection.call(zoom).call(drag);

    canvas.addEventListener("mousemove", (event) => {
      const [x, y] = transform.invert([event.offsetX, event.offsetY]);
      const node = findNode(x, y);
      if (onNodeHover) {
        onNodeHover(node, event);
      }
    });

    canvas.addEventListener("mouseleave", () => {
      if (onNodeHover) {
        onNodeHover(null);
      }
    });

    canvas.addEventListener("click", (event) => {
      const [x, y] = transform.invert([event.offsetX, event.offsetY]);
      const node = findNode(x, y);
      if (onNodeClick) {
        onNodeClick(node);
      }
    });

    window.addEventListener("resize", () => {
      resize();
      simulation.force("center", d3.forceCenter(width / 2, height / 2));
      simulation.alpha(0.4).restart();
    });

    requestAnimationFrame(draw);
  }

  function update(nodeMap, linkMap, options = {}) {
    nodes = Array.from(nodeMap.values());
    links = Array.from(linkMap.values());
    rebuildIndex();
    rebuildAggregatedLinks();
    hoverIndexBuiltAt = 0;
    const broadcastNode = nodeIndex.get(BROADCAST_ID);
    if (broadcastNode) {
      broadcastNode.isBroadcast = true;
      broadcastNode.fx = width / 2;
      broadcastNode.fy = height / 2;
    }

    if (simulation) {
      const jitterX = width * 0.4;
      const jitterY = height * 0.4;
      nodes.forEach((node) => {
        if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) {
          node.x = width / 2 + (Math.random() - 0.5) * jitterX;
          node.y = height / 2 + (Math.random() - 0.5) * jitterY;
        }
      });

      const sizeChanged = nodes.length !== lastNodeCount || links.length !== lastLinkCount;
      const shouldReheat = options.reheat || sizeChanged;
      simulation.nodes(nodes);
      simulation.force("link").links(links);
      applyDynamicForces();
      if (shouldReheat) {
        simulation.alpha(0.18).restart();
      } else {
        simulation.alphaTarget(0);
      }
      lastNodeCount = nodes.length;
      lastLinkCount = links.length;
    }
  }

  function pulse(nodeId, portnum, options = {}) {
    if (nodeId === null || nodeId === undefined) return;
    const kind = options.kind || "default";
    const profile = rippleProfiles[kind] || rippleProfiles.default;
    emitRipple({ nodeId }, portnum, { ...profile, ...options, kind });
  }

  function linkShockwave(sourceId, targetId, portnum, options = {}) {
    if (sourceId === null || sourceId === undefined) return;
    if (targetId === null || targetId === undefined) return;
    emitRipple(
      { sourceId, targetId },
      portnum,
      {
        kind: "link",
        duration: 880,
        maxRadius: 30,
        lineWidth: 1.6,
        alpha: 0.62,
        ringCount: 1,
        ringSpacing: 5,
        ringStagger: 60,
        stormRings: true,
        stormScale: 0.7,
        glow: true,
        ...options,
      },
    );
  }

  function setNodeClickHandler(handler) {
    onNodeClick = handler;
  }

  function setNodeHoverHandler(handler) {
    onNodeHover = handler;
  }

  function setBackgroundActionHandler(handler) {
    onBackgroundAction = handler;
  }

  function animateRoute(path, portnum, options = {}) {
    if (!Array.isArray(path) || path.length < 2) return;
    const stepMs = options.stepMs || ROUTE_STEP_MS;
    const fadeMs = options.fadeMs || ROUTE_FADE_MS;
    const color = options.color || ROUTE_COLOR_FORWARD;
    const direction = options.direction || "forward";
    routeAnimations.push({
      path,
      portnum,
      color,
      direction,
      start: performance.now(),
      stepMs,
      fadeMs,
    });
    path.forEach((nodeId, idx) => {
      const delay = idx * stepMs;
      window.setTimeout(() => {
        pulse(nodeId, portnum, { kind: "route", color });
      }, delay);
    });
  }

  init();
  return {
    update,
    pulse,
    linkShockwave,
    bumpActivity,
    addMessageBubble,
    setSelectedFocus,
    setBackgroundActionHandler,
    setNodeClickHandler,
    setNodeHoverHandler,
    animateRoute,
  };
})();

graph.setNodeClickHandler((node) => {
  if (node && node.id === BROADCAST_ID) {
    return;
  }
  if (node) {
    selectNode(node.id);
  } else {
    clearNodeSelection();
  }
});

graph.setNodeHoverHandler((node, event) => {
  if (!node) {
    graphTooltip.classList.remove("visible");
    return;
  }
  const lastSeen = node.lastSeenEpoch ? formatTime(node.lastSeenEpoch) : "--";
  graphTooltip.innerHTML = `<div>${escapeHtml(node.label || formatNodeId(node.id))}</div><div>Packets: ${escapeHtml(node.count || 0)}</div><div>Last seen: ${escapeHtml(lastSeen)}</div>`;
  graphTooltip.style.left = `${event.clientX + 12}px`;
  graphTooltip.style.top = `${event.clientY + 12}px`;
  graphTooltip.classList.add("visible");
});
graph.setBackgroundActionHandler(() => {
  clearNodeSelection();
});
async function bootstrap() {
  state.filters.window = Number(windowSelect.value);

  const health = await fetchJson("/api/health");
  if (health) {
    brokerValue.textContent = health.broker || "--";
    topicValue.textContent = health.topic || "--";
  }

  if (stressConfig) {
    state.connection = "live";
    updateLiveStatus();
    startStressTest(stressConfig);
    updateLegend(state.graphView.links);
    updateStats();
    return;
  }

  connectWs();
  await refreshAll();

  state.refreshTimer = setInterval(() => {
    if (!state.paused) {
      refreshSummary();
    }
  }, 15000);

}

bootstrap();
