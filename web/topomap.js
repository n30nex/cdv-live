
const state = {
  nodes: new Map(),
  nodeNames: new Map(),
  links: new Map(),
  paused: false,
  connection: "connecting",
  lastUpdate: null,
  socket: null,
  hasFit: false,
};

const viewState = {
  focusNodeId: null,
  focusNeighbors: new Set(),
  focusLinks: new Set(),
  focusRoutes: [],
  showGlow: true,
};

const routeHistory = [];

const BROADCAST_ID = 0xffffffff;
const LINK_FLASH_MS = 900;
const LINK_FADE_MS = 20000;
const LINK_HEAT_GAIN = 1;
const LINK_HEAT_MAX = 6;
const LINK_HEAT_HALF_LIFE_MS = 45000;
const ROUTING_PORTNUM = 5;
const TRACEROUTE_PORTNUM = 70;
const ROUTE_STEP_MS = 360;
const ROUTE_FADE_MS = 20000;
const ROUTE_COLOR_FORWARD = "#33ff79";
const ROUTE_COLOR_RETURN = "#ff3b3b";
const SEND_FLASH_MS = 5000;
const RECEIVE_FLASH_MS = 6000;
const DEFAULT_PITCH = 64;
const DEFAULT_BEARING = 0;
const DEFAULT_CENTER = [-79.4, 43.85];
const DEFAULT_ZOOM = 8.3;
const NODE_HEAT_GAIN = 0.7;
const NODE_HEAT_MAX = 4.5;
const NODE_HEAT_HALF_LIFE_MS = 3400;
const ACTIVITY_DECAY_MS = 4200;
const ACTIVITY_SPIKE_DECAY_MS = 1100;
const RIPPLE_RING_STAGGER_MS = 70;
const RIPPLE_RING_SPACING = 6;
const LINK_BASE_ALPHA = 0.38;
const LINK_FLASH_ALPHA = 0.95;
const LINK_BASE_WIDTH = 1.6;
const TRAIL_FADE_MS = 45000;
const TRAIL_MAX_COUNT = 1800;
const ROUTE_HISTORY_MAX = 200;
const FETCH_TIMEOUT_MS = 10000;
const HISTORY_PACKET_LIMIT = 500;
const HISTORY_WINDOW_SECONDS = 86400;
const HISTORY_BATCH_BUDGET_MS = 8;
const MAP_LOAD_TIMEOUT_MS = 15000;

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

const brokerValue = document.getElementById("brokerValue");
const topicValue = document.getElementById("topicValue");
const liveStatus = document.getElementById("liveStatus");
const pauseBtn = document.getElementById("pauseBtn");
const recenterBtn = document.getElementById("recenterBtn");
const mapStats = document.getElementById("mapStats");
const mapFocus = document.getElementById("mapFocus");
const mapTime = document.getElementById("mapTime");
const mapLegend = document.getElementById("mapLegend");
const pitchRange = document.getElementById("pitchRange");
const bearingRange = document.getElementById("bearingRange");
const pitchValue = document.getElementById("pitchValue");
const bearingValue = document.getElementById("bearingValue");
const cameraReset = document.getElementById("cameraReset");
const cameraNorth = document.getElementById("cameraNorth");
const cameraFlat = document.getElementById("cameraFlat");
const glowToggle = document.getElementById("glowToggle");
const loadingOverlay = document.getElementById("loadingOverlay");
const loadingStatus = document.getElementById("loadingStatus");
let loadingHidden = false;

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
  const hasNodes = state.nodes.size > 0 || state.nodeNames.size > 0;
  const hasTraffic = state.links.size > 0 || state.lastUpdate !== null;
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

function colorForPort(portnum) {
  if (portnum === null || portnum === undefined) {
    return "#5c6b7a";
  }
  return palette[Math.abs(Number(portnum)) % palette.length];
}

function colorForNode(nodeId) {
  if (nodeId === null || nodeId === undefined || nodeId === BROADCAST_ID) {
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
    { name: "Transmit arc", color: "rgba(95, 227, 208, 0.7)", swatchClass: "legend-rf" },
    { name: "Broadcast pulse", color: "rgba(141, 160, 178, 0.55)", swatchClass: "legend-broadcast" },
  ];

  const baseHtml = baseItems
    .map((item) => {
      return `<span class="legend-item"><span class="legend-swatch ${item.swatchClass}" style="background:${item.color}"></span>${item.name}</span>`;
    })
    .join("");

  const portHtml = items
    .map((item) => {
      const color = colorForPort(item.portnum);
      return `<span class="legend-item"><span class="legend-swatch" style="background:${color}"></span>${item.name}</span>`;
    })
    .join("");

  mapLegend.innerHTML = `${baseHtml}${portHtml}`;
}

function updateStats() {
  const positioned = Array.from(state.nodes.values()).filter((node) => hasPosition(node)).length;
  mapStats.textContent = `${positioned} positioned nodes`;
  mapTime.textContent = state.lastUpdate ? `Last update ${formatTime(state.lastUpdate)}` : "Last update -";
}

function updateFocusDisplay() {
  if (!mapFocus) return;
  if (viewState.focusNodeId === null || viewState.focusNodeId === undefined) {
    mapFocus.textContent = "Focus --";
    return;
  }
  const label = nodeLabel(viewState.focusNodeId);
  mapFocus.textContent = `Focus ${label}`;
}

function setFocusNode(nodeId) {
  if (nodeId === null || nodeId === undefined) {
    viewState.focusNodeId = null;
    viewState.focusNeighbors.clear();
    viewState.focusLinks.clear();
    viewState.focusRoutes = [];
    updateFocusDisplay();
    return;
  }
  if (nodeId === viewState.focusNodeId) {
    viewState.focusNodeId = null;
    viewState.focusNeighbors.clear();
    viewState.focusLinks.clear();
    viewState.focusRoutes = [];
    updateFocusDisplay();
    return;
  }
  viewState.focusNodeId = nodeId;
  viewState.focusNeighbors = new Set();
  viewState.focusLinks = new Set();
  state.links.forEach((link) => {
    if (link.sourceId === nodeId) {
      viewState.focusNeighbors.add(link.targetId);
      const key = `${Math.min(link.sourceId, link.targetId)}-${Math.max(link.sourceId, link.targetId)}`;
      viewState.focusLinks.add(key);
    } else if (link.targetId === nodeId) {
      viewState.focusNeighbors.add(link.sourceId);
      const key = `${Math.min(link.sourceId, link.targetId)}-${Math.max(link.sourceId, link.targetId)}`;
      viewState.focusLinks.add(key);
    }
  });
  const routes = routeHistory.filter((entry) => entry.path.includes(nodeId));
  viewState.focusRoutes = routes.slice(-5);
  updateFocusDisplay();
}

async function fetchJson(url, options = {}) {
  const timeoutMs = Number.isFinite(options.timeoutMs) ? options.timeoutMs : FETCH_TIMEOUT_MS;
  const hasAbort = typeof AbortController !== "undefined";
  const controller = hasAbort ? new AbortController() : null;
  const timeoutId = hasAbort && timeoutMs > 0
    ? setTimeout(() => controller.abort(), timeoutMs)
    : null;

  try {
    const response = await fetch(url, {
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
function ensureNode(nodeId, label) {
  if (nodeId === null || nodeId === undefined || nodeId === BROADCAST_ID) {
    return null;
  }
  const created = !state.nodes.has(nodeId);
  if (created) {
    state.nodes.set(nodeId, {
      id: nodeId,
      label: label || nodeLabel(nodeId),
      count: 0,
      lastActiveAt: 0,
      lastSeenEpoch: null,
      color: colorForNode(nodeId),
      heat: 0,
      lastHeatAt: 0,
      lastSendColor: null,
      lastReceiveColor: null,
      lat: null,
      lon: null,
      alt: null,
      lastPositionAt: 0,
    });
    markProjectionDirty();
  }
  const node = state.nodes.get(nodeId);
  if (!node.color) {
    node.color = colorForNode(nodeId);
  }
  if (label && node.label !== label) {
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

function parseNodeInfo(details) {
  if (!details || typeof details !== "object") return null;
  if (details.user && typeof details.user === "object") {
    return details.user;
  }
  return details;
}

function updateNodeName(nodeId, info) {
  if (nodeId === null || nodeId === undefined || !info) {
    return;
  }
  const label = info.short_name || info.long_name;
  if (!label) {
    return;
  }
  state.nodeNames.set(nodeId, label);
  const node = state.nodes.get(nodeId);
  if (node) {
    node.label = label;
  }
  if (viewState.focusNodeId === nodeId) {
    updateFocusDisplay();
  }
}

function coerceCoordinate(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const num = typeof value === "string" ? Number(value) : value;
  if (!Number.isFinite(num)) {
    return null;
  }
  return num;
}

function extractPosition(details) {
  if (!details || typeof details !== "object") {
    return null;
  }
  let lat = coerceCoordinate(details.latitude);
  let lon = coerceCoordinate(details.longitude);
  if (!Number.isFinite(lat) && Number.isFinite(details.latitude_i)) {
    lat = details.latitude_i / 1e7;
  }
  if (!Number.isFinite(lon) && Number.isFinite(details.longitude_i)) {
    lon = details.longitude_i / 1e7;
  }
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  const alt = coerceCoordinate(details.altitude);
  return { lat, lon, alt };
}

function hasPosition(node) {
  return node && Number.isFinite(node.lat) && Number.isFinite(node.lon);
}

function updateNodePosition(nodeId, details, timestamp) {
  const position = extractPosition(details);
  if (!position) {
    return false;
  }
  const node = ensureNode(nodeId, nodeLabel(nodeId));
  if (!node) {
    return false;
  }
  const changed =
    node.lat !== position.lat ||
    node.lon !== position.lon ||
    (position.alt !== null && position.alt !== undefined && node.alt !== position.alt);
  node.lat = position.lat;
  node.lon = position.lon;
  if (position.alt !== null && position.alt !== undefined) {
    node.alt = position.alt;
  }
  node.lastPositionAt = timestamp || node.lastPositionAt;
  if (changed) {
    markProjectionDirty();
  }
  return true;
}

function pathHasPositions(path) {
  return path.every((nodeId) => {
    const node = state.nodes.get(nodeId);
    return hasPosition(node);
  });
}

function buildPositionSegments(path) {
  const segments = [];
  let current = [];
  path.forEach((nodeId) => {
    const node = state.nodes.get(nodeId);
    if (hasPosition(node)) {
      if (!current.length || current[current.length - 1] !== nodeId) {
        current.push(nodeId);
      }
    } else if (current.length) {
      if (current.length > 1) {
        segments.push(current);
      }
      current = [];
    }
  });
  if (current.length > 1) {
    segments.push(current);
  }
  return segments;
}

function animateRouteForPacket(packet) {
  const paths = extractRoutePaths(packet);
  if (!paths.length) {
    return { hasRoutes: false };
  }
  let hasRoutes = false;
  paths.forEach((entry) => {
    const direction = entry.direction || "forward";
    const path = entry.path || [];
    const segments = buildPositionSegments(path);
    if (!segments.length) {
      const start = path[0];
      const end = path[path.length - 1];
      if (start !== undefined && end !== undefined && start !== end) {
        const startNode = state.nodes.get(start);
        const endNode = state.nodes.get(end);
        if (hasPosition(startNode) && hasPosition(endNode)) {
          segments.push([start, end]);
        }
      }
    }
    if (!segments.length) {
      return;
    }
    const color = direction === "return" ? ROUTE_COLOR_RETURN : ROUTE_COLOR_FORWARD;
    if (path.length > 1) {
      routeHistory.push({
        path,
        color,
        createdAt: packet.created_at || Math.floor(Date.now() / 1000),
      });
      if (routeHistory.length > ROUTE_HISTORY_MAX) {
        routeHistory.splice(0, routeHistory.length - ROUTE_HISTORY_MAX);
      }
      if (
        viewState.focusNodeId !== null &&
        viewState.focusNodeId !== undefined &&
        path.includes(viewState.focusNodeId)
      ) {
        const routes = routeHistory.filter((route) => route.path.includes(viewState.focusNodeId));
        viewState.focusRoutes = routes.slice(-5);
      }
    }
    segments.forEach((segment) => {
      if (segment.length < 2) {
        return;
      }
      overlay.animateRoute(segment, packet.portnum, {
        stepMs: ROUTE_STEP_MS,
        fadeMs: ROUTE_FADE_MS,
        color,
        direction,
      });
      for (let idx = 0; idx < segment.length - 1; idx += 1) {
        overlay.addRouteTrail(segment[idx], segment[idx + 1], color);
      }
      hasRoutes = true;
      overlay.markLinksDirty();
    });
  });
  return { hasRoutes };
}

function ingestPacket(packet, options = {}) {
  if (!packet) return;
  if (state.paused) return;
  const normalized = normalizePacket(packet);
  const now = performance.now();
  const source = normalized.from_id;
  const target = normalized.to_id;
  const portnum = normalized.portnum ?? null;
  const portname = normalized.portname || "UNKNOWN";
  const isBroadcast = target === BROADCAST_ID;
  const animate = options.animate !== false;
  const batch = options.batch === true;
  const includeRoutes = options.includeRoutes !== false;

  if (normalized.portname === "NODEINFO_APP") {
    const info = parseNodeInfo(normalized.details);
    updateNodeName(source, info);
  }

  const positionUpdated = updateNodePosition(source, normalized.details, normalized.created_at);
  if (positionUpdated && !state.hasFit) {
    fitMapToNodes();
  }

  const sourceNode = ensureNode(source, normalized.from_label);
  if (sourceNode) {
    sourceNode.count += 1;
    sourceNode.lastActiveAt = now;
    sourceNode.lastSeenEpoch = normalized.created_at;
    sourceNode.lastSentAt = now;
    sourceNode.lastSendColor = colorForPort(portnum);
    bumpNodeHeat(sourceNode, isBroadcast ? NODE_HEAT_GAIN * 0.55 : NODE_HEAT_GAIN, now);
  }

  let targetNode = null;
  if (!isBroadcast) {
    targetNode = ensureNode(target, normalized.to_label);
    if (targetNode) {
      targetNode.count += 1;
      targetNode.lastActiveAt = now;
      targetNode.lastSeenEpoch = normalized.created_at;
      targetNode.lastReceivedAt = now;
      targetNode.lastReceiveColor = colorForPort(portnum);
      bumpNodeHeat(targetNode, NODE_HEAT_GAIN * 0.9, now);
    }
  }

  const sourceHasPos = hasPosition(sourceNode);
  const targetHasPos = hasPosition(targetNode);
  const canPlot = sourceHasPos && targetHasPos;

  if (animate && isBroadcast && sourceHasPos) {
    overlay.bumpActivity(0.6);
    overlay.pulse(source, portnum, { kind: "broadcast" });
  }
  if (animate && !isBroadcast && canPlot) {
    overlay.bumpActivity(1);
    overlay.pulse(source, portnum, { kind: "send" });
    overlay.pulse(target, portnum, { kind: "receive" });
  }

  if (!isBroadcast && source !== null && target !== null && source !== undefined && target !== undefined) {
    const key = `${source}-${target}-${portnum}`;
    if (!state.links.has(key)) {
      overlay.markLinksDirty();
      state.links.set(key, {
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
    if (animate && canPlot) {
      overlay.addLinkTrail(source, target, portnum);
      link.flashUntil = now + LINK_FLASH_MS;
      const heatAge = link.lastHeatAt ? now - link.lastHeatAt : 0;
      const cooledHeat = link.heat ? link.heat * Math.exp(-heatAge / LINK_HEAT_HALF_LIFE_MS) : 0;
      link.heat = Math.min(LINK_HEAT_MAX, cooledHeat + LINK_HEAT_GAIN);
      link.lastHeatAt = now;
      overlay.linkShockwave(source, target, portnum);
    }
    if (viewState.focusNodeId !== null && viewState.focusNodeId !== undefined) {
      if (source === viewState.focusNodeId) {
        viewState.focusNeighbors.add(target);
        viewState.focusLinks.add(
          `${Math.min(source, target)}-${Math.max(source, target)}`,
        );
      } else if (target === viewState.focusNodeId) {
        viewState.focusNeighbors.add(source);
        viewState.focusLinks.add(
          `${Math.min(source, target)}-${Math.max(source, target)}`,
        );
      }
    }
  }

  if (includeRoutes) {
    animateRouteForPacket(normalized);
  }

  state.lastUpdate = normalized.created_at || Math.floor(Date.now() / 1000);
  if (!batch) {
    updateLegend(state.links);
    updateStats();
  }
  updateLoadingState();
}

function ingestPacketBatch(packets, options = {}) {
  if (!Array.isArray(packets) || packets.length === 0) {
    return Promise.resolve();
  }
  const total = packets.length;
  let index = 0;
  const ingestOptions = { ...options, batch: true };

  return new Promise((resolve) => {
    const step = () => {
      const start = performance.now();
      while (index < total && performance.now() - start < HISTORY_BATCH_BUDGET_MS) {
        ingestPacket(packets[index], ingestOptions);
        index += 1;
      }
      if (mapStats && index < total) {
        mapStats.textContent = `Loading history ${index}/${total}...`;
      }
      if (loadingStatus && index < total) {
        loadingStatus.textContent = `Loading history ${index}/${total}...`;
      }
      if (index < total) {
        requestAnimationFrame(step);
      } else {
        updateLegend(state.links);
        updateStats();
        updateLoadingState();
        resolve();
      }
    };
    requestAnimationFrame(step);
  });
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
    ingestPacket(packet, { animate: true });
  });

  socket.addEventListener("close", () => {
    state.connection = "disconnected";
    updateLiveStatus();
    setTimeout(connectWs, 2000);
  });
}

function setupPauseButton() {
  pauseBtn.addEventListener("click", () => {
    state.paused = !state.paused;
    pauseBtn.classList.toggle("active", state.paused);
    updateLiveStatus();
  });
}

function clampNumber(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function clampValue(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function syncCameraControls() {
  if (!pitchRange || !bearingRange) {
    return;
  }
  const pitch = Math.round(map.getPitch());
  const bearing = Math.round(map.getBearing());
  pitchRange.value = String(pitch);
  bearingRange.value = String(bearing);
  if (pitchValue) {
    pitchValue.textContent = `${pitch} deg`;
  }
  if (bearingValue) {
    bearingValue.textContent = `${bearing} deg`;
  }
}

function setupCameraControls() {
  if (!pitchRange || !bearingRange) {
    return;
  }

  const setPitch = (value, options = {}) => {
    const pitch = clampNumber(value, 0, 75);
    map.easeTo({ pitch, duration: options.duration ?? 120 });
  };
  const setBearing = (value, options = {}) => {
    map.easeTo({ bearing: value, duration: options.duration ?? 120 });
  };

  pitchRange.addEventListener("input", (event) => {
    const value = Number(event.target.value);
    if (Number.isFinite(value)) {
      setPitch(value, { duration: 0 });
    }
  });
  bearingRange.addEventListener("input", (event) => {
    const value = Number(event.target.value);
    if (Number.isFinite(value)) {
      setBearing(value, { duration: 0 });
    }
  });

  if (cameraReset) {
    cameraReset.addEventListener("click", () => {
      map.easeTo({ pitch: DEFAULT_PITCH, bearing: DEFAULT_BEARING, duration: 400 });
    });
  }
  if (cameraNorth) {
    cameraNorth.addEventListener("click", () => {
      map.easeTo({ bearing: 0, duration: 320 });
    });
  }
  if (cameraFlat) {
    cameraFlat.addEventListener("click", () => {
      map.easeTo({ pitch: 0, duration: 320 });
    });
  }
  if (glowToggle) {
    glowToggle.classList.toggle("active", viewState.showGlow);
    glowToggle.textContent = viewState.showGlow ? "Glow" : "Glow Off";
    glowToggle.addEventListener("click", () => {
      viewState.showGlow = !viewState.showGlow;
      glowToggle.classList.toggle("active", viewState.showGlow);
      glowToggle.textContent = viewState.showGlow ? "Glow" : "Glow Off";
    });
  }

map.on("pitch", () => {
  syncCameraControls();
  markProjectionDirty();
});
map.on("rotate", () => {
  syncCameraControls();
  markProjectionDirty();
});
map.on("moveend", () => {
  syncCameraControls();
  mapMoving = false;
  markProjectionDirty();
});
map.on("movestart", () => {
  mapMoving = true;
  markProjectionDirty();
});
map.on("move", markProjectionDirty);
map.on("zoom", markProjectionDirty);
map.on("click", (event) => {
  const picked = overlay.pickNode(event.point);
  if (picked) {
    setFocusNode(picked.id);
  } else {
    setFocusNode(null);
  }
});
  syncCameraControls();
}

function setupMiddleMouseCameraControls() {
  const container = map.getCanvasContainer();
  if (!container) {
    return;
  }
  let dragging = false;
  let startX = 0;
  let startY = 0;
  let startPitch = 0;
  let startBearing = 0;
  const sensitivity = 0.35;

  const onMouseMove = (event) => {
    if (!dragging) return;
    event.preventDefault();
    const dx = event.clientX - startX;
    const dy = event.clientY - startY;
    const nextBearing = startBearing + dx * sensitivity;
    const nextPitch = clampNumber(startPitch - dy * sensitivity, 0, 75);
    map.easeTo({ bearing: nextBearing, pitch: nextPitch, duration: 0 });
  };

  const stopDrag = () => {
    if (!dragging) return;
    dragging = false;
    document.removeEventListener("mousemove", onMouseMove);
    document.removeEventListener("mouseup", stopDrag);
    syncCameraControls();
  };

  container.addEventListener("mousedown", (event) => {
    if (event.button !== 1) {
      return;
    }
    event.preventDefault();
    dragging = true;
    startX = event.clientX;
    startY = event.clientY;
    startPitch = map.getPitch();
    startBearing = map.getBearing();
    document.addEventListener("mousemove", onMouseMove, { passive: false });
    document.addEventListener("mouseup", stopDrag);
  });

  container.addEventListener("mouseleave", stopDrag);
}

let mapMoving = false;

function markProjectionDirty() {
  if (typeof overlay !== "undefined" && overlay.markProjectionDirty) {
    overlay.markProjectionDirty();
  }
}

const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  center: DEFAULT_CENTER,
  zoom: DEFAULT_ZOOM,
  pitch: DEFAULT_PITCH,
  bearing: DEFAULT_BEARING,
  minPitch: 0,
  maxPitch: 75,
  antialias: true,
});

map.addControl(new maplibregl.NavigationControl({ showCompass: true }), "bottom-right");
map.dragRotate.enable();
map.touchZoomRotate.enableRotation();
map.touchPitch.enable();
map.keyboard.enable();

const FALLBACK_STYLE = {
  version: 8,
  sources: {},
  layers: [
    {
      id: "background",
      type: "background",
      paint: {
        "background-color": "#0b1218",
      },
    },
  ],
};

let fallbackApplied = false;
const mapLoadTimer = window.setTimeout(() => {
  if (!map.loaded() && !fallbackApplied) {
    fallbackApplied = true;
    map.setStyle(FALLBACK_STYLE, { diff: false });
  }
}, MAP_LOAD_TIMEOUT_MS);

function findFirstSymbolLayerId(style) {
  if (!style || !Array.isArray(style.layers)) {
    return null;
  }
  const layer = style.layers.find((entry) => entry.type === "symbol");
  return layer ? layer.id : null;
}

function addTerrain(mapInstance) {
  if (mapInstance.getSource("terrain")) {
    return;
  }
  mapInstance.addSource("terrain", {
    type: "raster-dem",
    url: "https://demotiles.maplibre.org/terrain-tiles/tiles.json",
    tileSize: 256,
    maxzoom: 12,
  });
  mapInstance.setTerrain({ source: "terrain", exaggeration: 1.35 });
  const beforeId = findFirstSymbolLayerId(mapInstance.getStyle());
  mapInstance.addLayer(
    {
      id: "terrain-hillshade",
      type: "hillshade",
      source: "terrain",
      paint: {
        "hillshade-exaggeration": 0.35,
      },
    },
    beforeId,
  );
  mapInstance.addLayer({
    id: "sky",
    type: "sky",
    paint: {
      "sky-type": "atmosphere",
      "sky-atmosphere-sun-intensity": 6,
    },
  });
}

function addBuildings(mapInstance) {
  const style = mapInstance.getStyle();
  if (!style || !Array.isArray(style.layers)) {
    return;
  }
  const buildingLayer = style.layers
    .slice()
    .reverse()
    .find((layer) => {
      const sourceLayer = layer["source-layer"];
      const id = layer.id ? String(layer.id).toLowerCase() : "";
      if (sourceLayer && String(sourceLayer).toLowerCase().includes("building")) {
        return true;
      }
      return id.includes("building");
    });
  if (!buildingLayer || !buildingLayer.source || mapInstance.getLayer("buildings-3d")) {
    return;
  }
  const beforeId = findFirstSymbolLayerId(style);
  const layer = {
    id: "buildings-3d",
    type: "fill-extrusion",
    source: buildingLayer.source,
    "source-layer": buildingLayer["source-layer"],
    minzoom: 13,
    paint: {
      "fill-extrusion-color": "rgba(20, 26, 34, 0.9)",
      "fill-extrusion-height": [
        "interpolate",
        ["linear"],
        ["zoom"],
        13,
        0,
        14,
        ["coalesce", ["get", "height"], ["get", "render_height"], 14],
        16,
        ["coalesce", ["get", "height"], ["get", "render_height"], 20],
      ],
      "fill-extrusion-base": [
        "interpolate",
        ["linear"],
        ["zoom"],
        13,
        0,
        14,
        ["coalesce", ["get", "min_height"], ["get", "render_min_height"], 0],
      ],
      "fill-extrusion-opacity": [
        "interpolate",
        ["linear"],
        ["zoom"],
        13,
        0,
        13.5,
        0.45,
        15,
        0.82,
      ],
      "fill-extrusion-vertical-gradient": true,
    },
  };
  if (beforeId) {
    mapInstance.addLayer(layer, beforeId);
  } else {
    mapInstance.addLayer(layer);
  }
}

function collectPositionBounds() {
  const bounds = new maplibregl.LngLatBounds();
  let hasBounds = false;
  state.nodes.forEach((node) => {
    if (!hasPosition(node)) {
      return;
    }
    bounds.extend([node.lon, node.lat]);
    hasBounds = true;
  });
  return hasBounds ? bounds : null;
}

function fitMapToNodes() {
  if (!map || !map.loaded()) {
    return;
  }
  const bounds = collectPositionBounds();
  if (!bounds) {
    return;
  }
  state.hasFit = true;
  map.fitBounds(bounds, {
    padding: { top: 160, left: 80, right: 80, bottom: 120 },
    duration: 1200,
    maxZoom: 13,
  });
}

recenterBtn.addEventListener("click", () => {
  fitMapToNodes();
});
const overlay = (() => {
  const canvas = document.getElementById("mapCanvas");
  const ctx = canvas.getContext("2d");
  const pulses = [];
  const routeAnimations = [];
  let aggregatedLinkGroups = [];
  let linkGroupsDirty = true;
  const linkTrails = [];
  const routeTrails = [];
  let width = 0;
  let height = 0;
  let activityEnergy = 0;
  let activitySpike = 0;
  let activityUpdatedAt = 0;
  let projectionDirty = true;
  let projectedCache = new Map();
  let lastDrawAt = 0;

  const rippleProfiles = {
    default: {
      duration: 900,
      maxRadius: 42,
      lineWidth: 2,
      alpha: 0.5,
      ringCount: 1,
      ringSpacing: RIPPLE_RING_SPACING,
      ringStagger: RIPPLE_RING_STAGGER_MS,
      stormRings: false,
      stormScale: 0.9,
      glow: true,
    },
    send: {
      duration: 960,
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

  function resize() {
    const rect = canvas.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    width = rect.width;
    height = rect.height;
    canvas.width = width * ratio;
    canvas.height = height * ratio;
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    projectionDirty = true;
  }

  const resizeObserver = new ResizeObserver(() => {
    resize();
  });
  resizeObserver.observe(canvas);
  window.addEventListener("resize", resize);
  resize();

  const degToRad = (value) => (value * Math.PI) / 180;

  function pitchCos() {
    return Math.cos(degToRad(map.getPitch()));
  }

  function pitchBoost() {
    return Math.max(0.2, Math.sin(degToRad(map.getPitch())));
  }

  function horizonY() {
    const pitch = map.getPitch();
    if (pitch < 6) {
      return -Infinity;
    }
    const t = Math.min(1, pitch / 75);
    return height * (0.12 + (1 - t) * 0.1);
  }

  function visibilityForY(y) {
    const horizon = horizonY();
    if (!Number.isFinite(horizon)) {
      return 1;
    }
    const fade = 140;
    const start = horizon - fade * 0.35;
    const end = horizon + fade;
    if (y <= start) {
      return 0;
    }
    if (y >= end) {
      return 1;
    }
    return (y - start) / (end - start);
  }

  function isInViewport(x, y, margin = 120) {
    return x >= -margin && x <= width + margin && y >= -margin && y <= height + margin;
  }

  function getLod() {
    const zoom = map.getZoom();
    return {
      zoom,
      showLabels: zoom >= 8.6,
      showDetails: zoom >= 8.1,
      showArrows: zoom >= 9,
      showTrails: zoom >= 7.4,
    };
  }

  function focusFactorForNode(nodeId) {
    if (viewState.focusNodeId === null || viewState.focusNodeId === undefined) {
      return 1;
    }
    if (nodeId === viewState.focusNodeId) {
      return 1;
    }
    if (viewState.focusNeighbors.has(nodeId)) {
      return 0.85;
    }
    return 0.12;
  }

  function focusFactorForLink(sourceId, targetId) {
    if (viewState.focusNodeId === null || viewState.focusNodeId === undefined) {
      return 1;
    }
    const key = `${Math.min(sourceId, targetId)}-${Math.max(sourceId, targetId)}`;
    if (viewState.focusLinks.has(key)) {
      return 1;
    }
    return 0.12;
  }

  function pruneTrails(list, now) {
    for (let i = list.length - 1; i >= 0; i -= 1) {
      if (now - list[i].startedAt > TRAIL_FADE_MS) {
        list.splice(i, 1);
      }
    }
  }

  function addTrail(list, sourceId, targetId, color) {
    list.push({ sourceId, targetId, color, startedAt: performance.now() });
    if (list.length > TRAIL_MAX_COUNT) {
      list.splice(0, list.length - TRAIL_MAX_COUNT);
    }
  }

  function addLinkTrail(sourceId, targetId, portnum) {
    if (sourceId === null || targetId === null || sourceId === undefined || targetId === undefined) {
      return;
    }
    const color = colorForPort(portnum);
    addTrail(linkTrails, sourceId, targetId, color);
  }

  function addRouteTrail(sourceId, targetId, color) {
    if (sourceId === null || targetId === null || sourceId === undefined || targetId === undefined) {
      return;
    }
    addTrail(routeTrails, sourceId, targetId, color);
  }

  function drawTrails(list, now, projected) {
    pruneTrails(list, now);
    list.forEach((trail) => {
      const source = projected.get(trail.sourceId);
      const target = projected.get(trail.targetId);
      if (!source || !target) {
        return;
      }
      const visibility = Math.min(source.visibility, target.visibility);
      if (visibility <= 0.05) {
        return;
      }
      const age = now - trail.startedAt;
      const fade = Math.max(0, 1 - age / TRAIL_FADE_MS);
      const control = controlPointForArc(source, target);
      const focusFactor = focusFactorForLink(trail.sourceId, trail.targetId);
      ctx.save();
      ctx.strokeStyle = trail.color;
      ctx.globalAlpha = 0.35 * fade * visibility * focusFactor;
      ctx.lineWidth = 3.2;
      if (viewState.showGlow) {
        ctx.shadowColor = trail.color;
        ctx.shadowBlur = 18 * fade;
      }
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      ctx.beginPath();
      ctx.moveTo(source.x, source.y);
      ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
      ctx.stroke();
      ctx.restore();
    });
  }

  function pickNode(point) {
    const x = point.x;
    const y = point.y;
    let best = null;
    let bestDist = Number.POSITIVE_INFINITY;
    projectedCache.forEach((entry) => {
      const radius = 6 + Math.log1p(entry.node.count || 1);
      const dx = entry.x - x;
      const dy = entry.y - y;
      const dist = Math.hypot(dx, dy);
      if (dist <= radius + 8 && dist < bestDist) {
        best = entry.node;
        bestDist = dist;
      }
    });
    return best;
  }

  function rebuildAggregatedLinks() {
    const grouped = new Map();
    state.links.forEach((link) => {
      if (link.sourceId === BROADCAST_ID || link.targetId === BROADCAST_ID) {
        return;
      }
      const a = Math.min(link.sourceId, link.targetId);
      const b = Math.max(link.sourceId, link.targetId);
      const key = `${a}-${b}`;
      let entry = grouped.get(key);
      if (!entry) {
        entry = { sourceId: a, targetId: b, linkRefs: [] };
        grouped.set(key, entry);
      }
      entry.linkRefs.push(link);
    });
    aggregatedLinkGroups = Array.from(grouped.values());
    linkGroupsDirty = false;
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

  function resolvePulsePosition(pulse, projected) {
    if (pulse.nodeId !== null && pulse.nodeId !== undefined) {
      const entry = projected.get(pulse.nodeId);
      if (!entry) {
        return null;
      }
      return { x: entry.x, y: entry.y, visibility: entry.visibility };
    }
    if (pulse.sourceId !== null && pulse.sourceId !== undefined &&
        pulse.targetId !== null && pulse.targetId !== undefined) {
      const source = projected.get(pulse.sourceId);
      const target = projected.get(pulse.targetId);
      if (!source || !target) {
        return null;
      }
      const visibility = Math.min(source.visibility, target.visibility);
      return {
        x: (source.x + target.x) / 2,
        y: (source.y + target.y) / 2,
        visibility,
      };
    }
    if (Number.isFinite(pulse.x) && Number.isFinite(pulse.y)) {
      return { x: pulse.x, y: pulse.y, visibility: visibilityForY(pulse.y) };
    }
    return null;
  }

  function projectNodes() {
    if (!projectionDirty && projectedCache.size) {
      return projectedCache;
    }
    const projected = new Map();
    state.nodes.forEach((node) => {
      if (!hasPosition(node)) {
        return;
      }
      const point = map.project([node.lon, node.lat]);
      const clampedX = clampValue(point.x, -120, width + 120);
      const clampedY = clampValue(point.y, -120, height + 120);
      const visibility = visibilityForY(point.y);
      if (visibility <= 0) {
        return;
      }
      projected.set(node.id, { x: clampedX, y: clampedY, node, visibility });
    });
    projectedCache = projected;
    projectionDirty = false;
    return projectedCache;
  }

  function controlPointForArc(source, target) {
    const dx = target.x - source.x;
    const dy = target.y - source.y;
    const distance = Math.hypot(dx, dy);
    if (!Number.isFinite(distance) || distance === 0) {
      return { x: (source.x + target.x) / 2, y: (source.y + target.y) / 2 };
    }
    const normX = -dy / distance;
    const normY = dx / distance;
    const offset = Math.min(140, Math.max(20, distance * 0.22));
    const lift = Math.min(280, Math.max(40, distance * 0.35)) * (0.35 + pitchBoost());
    return {
      x: (source.x + target.x) / 2 + normX * offset,
      y: (source.y + target.y) / 2 + normY * offset - lift,
    };
  }

  function pointOnQuadratic(p0, p1, p2, t) {
    const oneMinus = 1 - t;
    const a = oneMinus * oneMinus;
    const b = 2 * oneMinus * t;
    const c = t * t;
    return {
      x: a * p0.x + b * p1.x + c * p2.x,
      y: a * p0.y + b * p1.y + c * p2.y,
    };
  }

  function splitQuadratic(p0, p1, p2, t) {
    const p01 = {
      x: p0.x + (p1.x - p0.x) * t,
      y: p0.y + (p1.y - p0.y) * t,
    };
    const p12 = {
      x: p1.x + (p2.x - p1.x) * t,
      y: p1.y + (p2.y - p1.y) * t,
    };
    const p012 = {
      x: p01.x + (p12.x - p01.x) * t,
      y: p01.y + (p12.y - p01.y) * t,
    };
    return { control: p01, end: p012 };
  }

  function quadraticTangentAngle(p0, p1, p2, t) {
    const dx = 2 * (1 - t) * (p1.x - p0.x) + 2 * t * (p2.x - p1.x);
    const dy = 2 * (1 - t) * (p1.y - p0.y) + 2 * t * (p2.y - p1.y);
    return Math.atan2(dy, dx);
  }

  function drawArrowheadAt(x, y, angle, size, color, alpha) {
    if (!Number.isFinite(angle)) {
      return;
    }
    const back = size * 1.4;
    const wing = size * 0.7;
    ctx.save();
    ctx.translate(x, y);
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
  function drawLinks(now, nowEpoch, projected, lod) {
    if (linkGroupsDirty) {
      rebuildAggregatedLinks();
    }

    aggregatedLinkGroups.forEach((group) => {
      const source = projected.get(group.sourceId);
      const target = projected.get(group.targetId);
      if (!source || !target) {
        return;
      }
      const visibility = Math.min(source.visibility, target.visibility);
      if (visibility <= 0.05) {
        return;
      }
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

      const color = colorForPort(portnum);
      const flashRemaining = lod.showDetails && flashUntil
        ? Math.max(0, Math.min(1, (flashUntil - now) / LINK_FLASH_MS))
        : 0;
      const ageMs = lastSeen ? (nowEpoch - lastSeen) * 1000 : Number.POSITIVE_INFINITY;
      const fadeFactor = lastSeen ? Math.max(0, Math.min(1, 1 - ageMs / LINK_FADE_MS)) : 0;
      const heatNormalized = heat ? Math.min(1, heat / LINK_HEAT_MAX) : 0;
      const heatVisibility = Math.max(fadeFactor, heatNormalized);
      const linkVisibility = Math.max(heatVisibility, flashRemaining);
      if (linkVisibility <= 0.01) {
        return;
      }
      const baseWidth = (lod.showDetails ? LINK_BASE_WIDTH : LINK_BASE_WIDTH * 0.75)
        + Math.log1p(count || 1) * 0.6;
      const focusFactor = focusFactorForLink(group.sourceId, group.targetId);
      const baseAlpha = LINK_BASE_ALPHA;
      const flashAlpha = LINK_FLASH_ALPHA;
      const alpha =
        (baseAlpha + (flashAlpha - baseAlpha) * flashRemaining) *
        (flashRemaining > 0 ? 1 : heatVisibility) *
        visibility *
        linkVisibility *
        focusFactor;
      const width = baseWidth + 3 * flashRemaining;
      const control = controlPointForArc(source, target);

      if (viewState.showGlow) {
        ctx.save();
        ctx.strokeStyle = color;
        ctx.globalAlpha = Math.min(1, alpha * 0.55);
        ctx.lineWidth = width + 4;
        ctx.lineCap = "round";
        ctx.lineJoin = "round";
        ctx.shadowColor = color;
        ctx.shadowBlur = 18;
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
        ctx.stroke();
        ctx.restore();
      }

      if (flashRemaining > 0.05) {
        ctx.save();
        ctx.strokeStyle = color;
        ctx.globalAlpha = alpha;
        ctx.lineWidth = width + 2;
        ctx.lineCap = "round";
        ctx.lineJoin = "round";
        ctx.shadowColor = color;
        ctx.shadowBlur = viewState.showGlow ? 12 + flashRemaining * 14 : 0;
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
        ctx.stroke();
        ctx.restore();
      }

      ctx.save();
      ctx.strokeStyle = color;
      ctx.globalAlpha = alpha;
      ctx.lineWidth = width;
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      ctx.beginPath();
      ctx.moveTo(source.x, source.y);
      ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
      ctx.stroke();
      ctx.restore();

      if (lod.showDetails && flashRemaining > 0.05) {
        const t = 1 - flashRemaining;
        const point = pointOnQuadratic(source, control, target, t);
        ctx.save();
        ctx.fillStyle = color;
        ctx.globalAlpha = 0.85 * flashRemaining;
        ctx.beginPath();
        ctx.arc(point.x, point.y, 3.5 + flashRemaining * 2, 0, Math.PI * 2);
        ctx.fill();
        ctx.restore();
      }

      if (
        (viewState.focusNodeId !== null && viewState.focusNodeId !== undefined) &&
        focusFactor > 0.9
      ) {
        ctx.save();
        ctx.strokeStyle = color;
        ctx.globalAlpha = Math.min(1, alpha * 1.1);
        ctx.lineWidth = width + 1.8;
        ctx.lineCap = "round";
        ctx.lineJoin = "round";
        if (viewState.showGlow) {
          ctx.shadowColor = color;
          ctx.shadowBlur = 16;
        }
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
        ctx.stroke();
        ctx.restore();
      }
    });
  }

  function drawRouteAnimations(now, projected, lod) {
    const focusDim = viewState.focusNodeId === null || viewState.focusNodeId === undefined ? 1 : 0.25;
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
      ctx.shadowColor = color;
      ctx.shadowBlur = viewState.showGlow ? 14 : 0;

      for (let idx = 0; idx <= activeIndex; idx += 1) {
        const source = projected.get(anim.path[idx]);
        const target = projected.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        const segmentVisibility = Math.min(source.visibility, target.visibility);
        if (segmentVisibility <= 0.05) {
          continue;
        }
        const control = controlPointForArc(source, target);
        ctx.globalAlpha = 0.75 * fadeFactor * segmentVisibility * focusDim;
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        if (idx === activeIndex && activeIndex < totalSegments) {
          const partial = splitQuadratic(source, control, target, activeT);
          ctx.quadraticCurveTo(partial.control.x, partial.control.y, partial.end.x, partial.end.y);
        } else {
          ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
        }
        ctx.stroke();
      }

      if (lod.showArrows) {
        const arrowSize = 6;
        for (let idx = 0; idx <= activeIndex; idx += 1) {
          const source = projected.get(anim.path[idx]);
          const target = projected.get(anim.path[idx + 1]);
          if (!source || !target) continue;
          const segmentVisibility = Math.min(source.visibility, target.visibility);
          if (segmentVisibility <= 0.05) {
            continue;
          }
          const control = controlPointForArc(source, target);
          if (idx === activeIndex && activeIndex < totalSegments) {
            if (activeT < 0.2) {
              continue;
            }
            const arrowT = Math.min(0.9, activeT * 0.85);
            const point = pointOnQuadratic(source, control, target, arrowT);
            const angle = quadraticTangentAngle(source, control, target, arrowT);
            drawArrowheadAt(
              point.x,
              point.y,
              angle,
              arrowSize,
              color,
              0.85 * fadeFactor * segmentVisibility * focusDim,
            );
          } else {
            const point = pointOnQuadratic(source, control, target, 0.78);
            const angle = quadraticTangentAngle(source, control, target, 0.78);
            drawArrowheadAt(
              point.x,
              point.y,
              angle,
              arrowSize,
              color,
              0.7 * fadeFactor * segmentVisibility * focusDim,
            );
          }
        }
      }

      ctx.shadowBlur = 0;
      ctx.lineWidth = 2;
      ctx.setLineDash([6, 8]);
      for (let idx = 0; idx < activeIndex; idx += 1) {
        const source = projected.get(anim.path[idx]);
        const target = projected.get(anim.path[idx + 1]);
        if (!source || !target) continue;
        const segmentVisibility = Math.min(source.visibility, target.visibility);
        if (segmentVisibility <= 0.05) {
          continue;
        }
        const control = controlPointForArc(source, target);
        ctx.globalAlpha = 0.4 * fadeFactor * segmentVisibility * focusDim;
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
        ctx.stroke();
      }
      ctx.setLineDash([]);

      if (lod.showDetails) {
        for (let idx = 0; idx <= activeIndex; idx += 1) {
          const node = projected.get(anim.path[idx]);
          if (!node) continue;
          if (node.visibility <= 0.05) {
            continue;
          }
          ctx.globalAlpha = 0.65 * fadeFactor * node.visibility * focusDim;
          ctx.beginPath();
          ctx.arc(node.x, node.y, 3.5, 0, Math.PI * 2);
          ctx.fillStyle = color;
          ctx.fill();
        }
      }
      ctx.restore();

      if (lod.showDetails && viewState.showGlow) {
        ctx.save();
        ctx.fillStyle = rgbaFromHex(color, 0.9);
        ctx.shadowColor = rgbaFromHex(color, 0.75);
        ctx.shadowBlur = 12;
        const sparkleCount = 2;
        for (let idx = 0; idx <= activeIndex; idx += 1) {
          const source = projected.get(anim.path[idx]);
          const target = projected.get(anim.path[idx + 1]);
          if (!source || !target) continue;
          const segmentVisibility = Math.min(source.visibility, target.visibility);
          if (segmentVisibility <= 0.05) {
            continue;
          }
          const control = controlPointForArc(source, target);
          const segEndT = idx === activeIndex ? activeT : 1;
          for (let s = 0; s < sparkleCount; s += 1) {
            const phase = (now / 220 + idx * 0.37 + s * 0.45) % 1;
            const t = Math.min(segEndT, phase);
            if (t <= 0) continue;
            const pos = pointOnQuadratic(source, control, target, t);
            const twinkle = 0.6 + 0.4 * Math.sin((now / 120) + idx + s);
            ctx.beginPath();
            ctx.globalAlpha = 0.7 * fadeFactor * twinkle * segmentVisibility * focusDim;
            ctx.arc(pos.x, pos.y, 2.2 + twinkle, 0, Math.PI * 2);
            ctx.fill();
          }
        }
        ctx.restore();
      }

      if (lod.showDetails) {
        ctx.save();
        ctx.font = "10px Space Grotesk";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        for (let idx = 0; idx <= activeIndex; idx += 1) {
          const source = projected.get(anim.path[idx]);
          const target = projected.get(anim.path[idx + 1]);
          if (!source || !target) continue;
          const segmentVisibility = Math.min(source.visibility, target.visibility);
          if (segmentVisibility <= 0.05) {
            continue;
          }
          const control = controlPointForArc(source, target);
          const segEndT = idx === activeIndex ? activeT : 1;
          const t = Math.max(0.2, Math.min(0.8, segEndT * 0.6 + 0.2));
          const pos = pointOnQuadratic(source, control, target, t);
          const radius = 7;
          const label = String(idx + 1);
          ctx.globalAlpha = 0.85 * fadeFactor * segmentVisibility * focusDim;
          ctx.fillStyle = "rgba(11, 16, 22, 0.85)";
          ctx.beginPath();
          ctx.arc(pos.x, pos.y - 10, radius, 0, Math.PI * 2);
          ctx.fill();
          ctx.strokeStyle = color;
          ctx.lineWidth = 1;
          ctx.stroke();
          ctx.fillStyle = "rgba(255, 255, 255, 0.95)";
          ctx.fillText(label, pos.x, pos.y - 9.5);
        }
        ctx.restore();
      }
    }
  }

  function drawFocusRoutes(projected, lod) {
    if (!viewState.focusRoutes.length) {
      return;
    }
    ctx.save();
    ctx.setLineDash([6, 8]);
    viewState.focusRoutes.forEach((entry) => {
      const path = entry.path || [];
      if (path.length < 2) {
        return;
      }
      const color = entry.color || ROUTE_COLOR_FORWARD;
      for (let idx = 0; idx < path.length - 1; idx += 1) {
        const source = projected.get(path[idx]);
        const target = projected.get(path[idx + 1]);
        if (!source || !target) continue;
        const segmentVisibility = Math.min(source.visibility, target.visibility);
        if (segmentVisibility <= 0.05) {
          continue;
        }
        const control = controlPointForArc(source, target);
        ctx.strokeStyle = color;
        ctx.globalAlpha = 0.85 * segmentVisibility;
        ctx.lineWidth = lod.showDetails ? 3.2 : 2.4;
        ctx.lineCap = "round";
        ctx.lineJoin = "round";
        if (viewState.showGlow) {
          ctx.shadowColor = color;
          ctx.shadowBlur = 14;
        } else {
          ctx.shadowBlur = 0;
        }
        ctx.beginPath();
        ctx.moveTo(source.x, source.y);
        ctx.quadraticCurveTo(control.x, control.y, target.x, target.y);
        ctx.stroke();
      }
    });
    ctx.setLineDash([]);
    ctx.restore();
  }
  function drawNodeHeat(now, projected) {
    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    projected.forEach((entry) => {
      const node = entry.node;
      const heatAge = node.lastHeatAt ? now - node.lastHeatAt : Number.POSITIVE_INFINITY;
      const heat = node.heat ? node.heat * Math.exp(-heatAge / NODE_HEAT_HALF_LIFE_MS) : 0;
      if (heat <= 0.02) {
        return;
      }
      const intensity = Math.min(1, (heat / NODE_HEAT_MAX) * entry.visibility * focusFactorForNode(node.id));
      if (intensity <= 0.02) {
        return;
      }
      const radius = 16 + intensity * 30 + Math.log1p(node.count || 1) * 2;
      const color = node.color || colorForNode(node.id);
      const gradient = ctx.createRadialGradient(entry.x, entry.y, 0, entry.x, entry.y, radius);
      gradient.addColorStop(0, rgbaFromHex(color, 0.22 * intensity));
      gradient.addColorStop(0.4, rgbaFromHex(color, 0.12 * intensity));
      gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
      ctx.fillStyle = gradient;
      ctx.beginPath();
      ctx.arc(entry.x, entry.y, radius, 0, Math.PI * 2);
      ctx.fill();
    });
    ctx.restore();
  }

  function drawPulses(now, projected) {
    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    const ellipseScale = Math.max(0.25, pitchCos());
    for (let i = pulses.length - 1; i >= 0; i -= 1) {
      const pulse = pulses[i];
      const pos = resolvePulsePosition(pulse, projected);
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
      const visibility = pos.visibility ?? 1;
      if (visibility <= 0.05) {
        continue;
      }
      let focusFactor = 1;
      if (pulse.nodeId !== null && pulse.nodeId !== undefined) {
        focusFactor = focusFactorForNode(pulse.nodeId);
      } else if (pulse.sourceId !== null && pulse.targetId !== null) {
        focusFactor = focusFactorForLink(pulse.sourceId, pulse.targetId);
      }
      const alpha = (pulse.alpha || 0.5) * fade * visibility * focusFactor;
      const lineWidth = (pulse.lineWidth || 2) * (0.85 + 0.2 * fade);
      const isBroadcast = pulse.kind === "broadcast" || pulse.kind === "broadcast-core";

      ctx.save();
      ctx.translate(pos.x, pos.y);
      ctx.scale(1, ellipseScale);
      if (pulse.glow && viewState.showGlow) {
        ctx.shadowColor = pulse.color;
        ctx.shadowBlur = 8 + 12 * fade;
      } else {
        ctx.shadowBlur = 0;
      }

      if (isBroadcast) {
        const gradient = ctx.createRadialGradient(0, 0, 0, 0, 0, radius);
        gradient.addColorStop(0, rgbaFromHex(pulse.color, 0.22 * alpha));
        gradient.addColorStop(0.45, rgbaFromHex(pulse.color, 0.12 * alpha));
        gradient.addColorStop(1, "rgba(0, 0, 0, 0)");
        ctx.fillStyle = gradient;
        ctx.beginPath();
        ctx.arc(0, 0, radius, 0, Math.PI * 2);
        ctx.fill();

        ctx.strokeStyle = pulse.color;
        ctx.globalAlpha = Math.min(1, alpha + 0.25);
        ctx.lineWidth = lineWidth;
        ctx.beginPath();
        ctx.arc(0, 0, radius, 0, Math.PI * 2);
        ctx.stroke();
      } else {
        ctx.strokeStyle = pulse.color;
        ctx.globalAlpha = alpha;
        ctx.lineWidth = lineWidth;
        ctx.beginPath();
        ctx.arc(0, 0, radius, 0, Math.PI * 2);
        ctx.stroke();
      }
      ctx.restore();
    }
    ctx.restore();
  }

  function drawNodes(now, nowEpoch, projected, lod) {
    ctx.save();
    ctx.font = "11px Space Grotesk";
    ctx.textBaseline = "middle";
    projected.forEach((entry) => {
      const node = entry.node;
      const baseRadius = 5 + Math.log1p(node.count || 1);
      const nodeColor = node.color || colorForNode(node.id);
      const visibility = entry.visibility ?? 1;
      if (visibility <= 0.05) {
        return;
      }
      const lastSeenAge = node.lastSeenEpoch ? nowEpoch - node.lastSeenEpoch : null;
      const ageFactor = lastSeenAge !== null ? Math.max(0.3, Math.min(1, 1 - lastSeenAge / 3600)) : 0.8;
      const altBoost = Number.isFinite(node.alt) ? Math.min(0.25, Math.max(0, node.alt / 1200)) : 0;
      const radius = baseRadius * (0.75 + 0.25 * ageFactor) * (1 + altBoost);
      const focusFactor = focusFactorForNode(node.id);
      const alphaFactor = visibility * ageFactor * focusFactor;

      if (lod.showDetails && lastSeenAge !== null) {
        let ringColor = null;
        if (lastSeenAge < 120) {
          ringColor = rgbaFromHex(nodeColor, 0.18);
        } else if (lastSeenAge < 600) {
          ringColor = rgbaFromHex(nodeColor, 0.1);
        }
        if (ringColor) {
          ctx.fillStyle = ringColor;
          ctx.globalAlpha = alphaFactor;
          ctx.beginPath();
          ctx.arc(entry.x, entry.y, radius + 5, 0, Math.PI * 2);
          ctx.fill();
        }
      }

      const sentAge = node.lastSentAt ? now - node.lastSentAt : null;
      if (lod.showDetails && sentAge !== null && sentAge < SEND_FLASH_MS) {
        const t = sentAge / SEND_FLASH_MS;
        const ringColor = node.lastSendColor || nodeColor;
        ctx.strokeStyle = rgbaFromHex(ringColor, 0.55 * (1 - t));
        ctx.globalAlpha = alphaFactor;
        ctx.lineWidth = 1.4;
        ctx.beginPath();
        ctx.arc(entry.x, entry.y, radius + 6 + t * 6, 0, Math.PI * 2);
        ctx.stroke();
      }

      const receiveAge = node.lastReceivedAt ? now - node.lastReceivedAt : null;
      if (lod.showDetails && receiveAge !== null && receiveAge < RECEIVE_FLASH_MS) {
        const t = receiveAge / RECEIVE_FLASH_MS;
        const ringColor = node.lastReceiveColor || nodeColor;
        ctx.strokeStyle = rgbaFromHex(ringColor, 0.5 * (1 - t));
        ctx.globalAlpha = alphaFactor;
        ctx.lineWidth = 1.3;
        ctx.beginPath();
        ctx.arc(entry.x, entry.y, radius + 10 + t * 6, 0, Math.PI * 2);
        ctx.stroke();
      }

      ctx.save();
      ctx.globalAlpha = 0.25 * alphaFactor;
      ctx.fillStyle = "rgba(0, 0, 0, 0.6)";
      ctx.beginPath();
      ctx.arc(entry.x + 2, entry.y + 2, radius + 1, 0, Math.PI * 2);
      ctx.fill();
      ctx.restore();

      ctx.globalAlpha = alphaFactor;
      ctx.fillStyle = rgbaFromHex(nodeColor, 0.85);
      ctx.beginPath();
      ctx.arc(entry.x, entry.y, radius, 0, Math.PI * 2);
      ctx.fill();

      ctx.strokeStyle = rgbaFromHex(nodeColor, 0.55);
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.arc(entry.x, entry.y, radius + 1.2, 0, Math.PI * 2);
      ctx.stroke();

      if (lod.showLabels && focusFactor > 0.2) {
        ctx.fillStyle = "rgba(200, 220, 235, 0.95)";
        ctx.fillText(node.label || node.id, entry.x + radius + 5, entry.y + 3);
      }
    });
    ctx.restore();
  }

  function draw() {
    if (!map || !map.loaded()) {
      requestAnimationFrame(draw);
      return;
    }
    const now = performance.now();
    if (mapMoving && now - lastDrawAt < 50) {
      requestAnimationFrame(draw);
      return;
    }
    lastDrawAt = now;
    const nowEpoch = Date.now() / 1000;
    const projected = projectNodes();
    const lod = getLod();
    ctx.clearRect(0, 0, width, height);

    if (lod.showTrails) {
      drawTrails(linkTrails, now, projected);
      drawTrails(routeTrails, now, projected);
    }
    drawLinks(now, nowEpoch, projected, lod);
    drawRouteAnimations(now, projected, lod);
    drawFocusRoutes(projected, lod);
    drawNodeHeat(now, projected);
    drawPulses(now, projected);
    drawNodes(now, nowEpoch, projected, lod);

    requestAnimationFrame(draw);
  }

  requestAnimationFrame(draw);
  return {
    pulse,
    linkShockwave,
    animateRoute,
    bumpActivity,
    addLinkTrail,
    addRouteTrail,
    pickNode,
    markProjectionDirty: () => {
      projectionDirty = true;
    },
    markLinksDirty: () => {
      linkGroupsDirty = true;
    },
  };
})();
async function bootstrap() {
  updateLiveStatus();
  setupPauseButton();
  updateStats();
  updateFocusDisplay();

  connectWs();

  const healthPromise = fetchJson("/api/health");
  const nodesPromise = fetchJson(`/api/nodes?window=${HISTORY_WINDOW_SECONDS}`);
  const packetsPromise = fetchJson(
    `/api/packets?limit=${HISTORY_PACKET_LIMIT}&window=${HISTORY_WINDOW_SECONDS}`,
  );

  const [health, nodesData, packetsData] = await Promise.all([
    healthPromise,
    nodesPromise,
    packetsPromise,
  ]);

  if (health) {
    brokerValue.textContent = health.broker || "--";
    topicValue.textContent = health.topic || "--";
  }

  if (Array.isArray(nodesData)) {
    nodesData.forEach((node) => {
      const label = nodeLabelFromInfo(node.node_id, node);
      state.nodeNames.set(node.node_id, label);
    });
  }

  let historyLoaded = false;
  if (Array.isArray(packetsData)) {
    historyLoaded = true;
    const history = packetsData.slice().reverse();
    await ingestPacketBatch(history, { animate: false, includeRoutes: false });
  }

  if (!historyLoaded) {
    updateLegend(state.links);
    updateStats();
  }
  updateLoadingState();
}

map.on("load", () => {
  if (mapLoadTimer) {
    clearTimeout(mapLoadTimer);
  }
  if (!fallbackApplied) {
    addTerrain(map);
    addBuildings(map);
  }
  setupCameraControls();
  setupMiddleMouseCameraControls();
  bootstrap();
});
